import grpc
import sys
import threading
import time
from concurrent import futures
from pathlib import Path
import argparse
import csv
import json
import hashlib

# Add project root to Python path
project_root = str(Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# fmt: off
from generated import coordinator_pb2 # isort: skip
from generated import coordinator_pb2_grpc # isort: skip
from generated import fileserver_pb2 # isort: skip
from generated import fileserver_pb2_grpc # isort: skip
# fmt: on


class CoordinatorServer(coordinator_pb2_grpc.CoordinatorServicer,
                        coordinator_pb2_grpc.CoordinatorInternalServicer):

    def __init__(
        self,
        address: str,
        port: int,
        file_list_path: str,
        fileserver_addresses: list[str],
        client_response_timeout: int = 15,
        server_id: int = 1,
        replica_server_address: str = None,
        snapshot_interval: int = 60,
    ):
        self.address = address  # Server address
        self.port = port  # Server port
        # self.heartbeat_interval = heartbeat_interval  # Heartbeat interval in seconds
        # Path to the file containing list of files to process
        self.file_list_path = file_list_path
        self.file_list = []  # List of files to process
        self.file_size = []  # List of file sizes
        self.lock = threading.RLock()  # Add a lock for thread safety
        # Time in seconds to wait before considering a client dead
        self.client_response_timeout = client_response_timeout
        self.processing_complete = False  # Flag to indicate if processing is complete

        self.server_id = server_id  # Unique server ID
        # For primary-replica setup
        self.primary_server = None  # Is this the primary server
        # Address of the replica server
        self.replica_server_address = replica_server_address
        self.last_primary_heartbeat = 0  # Last heartbeat time from primary server

        # Snapshot stuff
        # Interval in seconds to take snapshots
        self.coordinator_addresses = fileserver_addresses
        self.snapshot_interval = snapshot_interval
        self.last_snapshot_time = time.time()  # Last snapshot time

        self.server_uptime = time.time()  # Server start time

        self.snapshot = {}  # To store snapshot data

        self.snapshot_tracker = {}  # To track snapshot status of clients

        # State dictionaries to track file processing status
        self.state = {
            "pending_prime": [],
            "in_progress_prime": [],
            "completed_prime": [],
            "pending_de_duplication": [],
            "in_progress_de_duplication": [],
            "completed_de_duplication": [],
        }
        self.result_filename = ""  # To store the final result filename

        # To track connected clients/workers {client_id: [alive_status, last_heartbeat_time, assigned_task_id, snapshot_bool, snapshot_channel_recording]}
        self.clients = {}

        self.tasks = {}  # To track tasks {task_id: Task object}

    def initialize(self):
        """
        Initizalizes the server by connecting to replica server and determining role, 
        loading the file list, setting up initial state.
        """
        print(
            f"[Coordinator {self.server_id}] Initializing Coordinator Server at {self.address}:{self.port}")
        self.connect_to_primary()
        if self.primary_server:
            snapshot_data = self.get_gloabl_snapshot_from_fs()
            if snapshot_data:
                print(
                    f"[Coordinator {self.server_id}] Restoring state from retrieved snapshot.")
                self.restore_state(snapshot_data["coordinator"])
            else:
                self.load_file_list()
                self.initialize_files_for_processing()
                print(
                    f"[Coordinator {self.server_id}] Number of files to process: {len(self.state['pending_prime'])}")

    def register_client(self, client_id: str):
        """
        Registers a new client with the server (adds client id to the clients dictionary), and start heartbeat.
        """
        with self.lock:
            if client_id not in self.clients:
                print(
                    f"[Coordinator {self.server_id}] Registering new client: {client_id}")
                # [alive_status, last_heartbeat_time, assigned_task_id, snapshot_bool, snapshot_channel_recording]
                self.clients[client_id] = [True, time.time(), 0, False, False]
            else:
                print(
                    f"[Coordinator {self.server_id}] Client {client_id} is already registered.")

            # Start heartbeat monitoring for the client

    def load_file_list(self):
        """
        Loads the file list from a CSV and adds first column to self.file_list and state['pending_prime'],
        and second column to self.file_size. The CSV is expected to contain filenames in the first column
        and optional file sizes in the second column.
        """
        with self.lock:
            print(
                f"[Coordinator {self.server_id}] Loading file list from {self.file_list_path}")
            try:
                with open(self.file_list_path, 'r', newline='') as f:
                    reader = csv.reader(f)

                    first_row = reader.__next__()
                    # Check if first row is header
                    if first_row[0].lower() == 'file_name':
                        pass  # Skip header row
                    else:
                        # Process first row
                        if first_row:
                            self.file_list.append(first_row[0].strip())
                            self.file_size.append(int(first_row[1].strip()))
                    # Process remaining rows
                    for row in reader:
                        if not row:
                            continue  # Skip empty rows
                        # Get filename
                        self.file_list.append(row[0].strip())
                        # Get file size
                        self.file_size.append(int(row[1].strip()))

                print(
                    f"[Coordinator {self.server_id}] Read {len(self.file_list)} files.")
            except Exception as e:
                print(
                    f"[Coordinator {self.server_id}] Error loading file list: {e}")
                sys.exit(1)

    def initialize_files_for_processing(self):
        """Adds the first list of files to queue for processing divides them based on number of files

        Args:
            files (list): list of all files
        """

        # get average numbers per file
        with self.lock:
            num_files = max(len(self.file_list), 3)
            tot_numbers = sum(self.file_size)
            avg_numbers_per_file = int(
                tot_numbers / num_files) if num_files > 0 else 0
            print(
                f"[Coordinator {self.server_id}] Average numbers per worker: {avg_numbers_per_file}, Total files: {num_files}, Total numbers: {tot_numbers}")

            # Distribute files into pending_prime based on average size
            for filename, size in zip(self.file_list, self.file_size):

                if size <= avg_numbers_per_file:
                    self.state['pending_prime'].append((filename, 0, size))
                else:
                    size_temp = size
                    i = 1
                    while size_temp > avg_numbers_per_file:
                        self.state['pending_prime'].append(
                            (filename, size - size_temp, int(avg_numbers_per_file) * i))
                        size_temp -= avg_numbers_per_file
                        i += 1
                    if size_temp > 0:
                        self.state['pending_prime'].append(
                            (filename, size - size_temp, int(size)))

    def check_processing_complete(self):
        """Checks if all processing is completed
        Returns:
            Boolean: true if completed, false otherwise
        """
        with self.lock:
            # check if there are no pending or in-progress tasks and at most one pending de-duplication file
            if (not self.state['pending_prime'] and
                    not self.state['in_progress_prime'] and
                    not self.state['in_progress_de_duplication'] and
                    len(self.state['pending_de_duplication']) == 1):
                self.processing_complete = True
                return True
            return False

    def create_task(self, client_id: str):
        """Creates a task Obejct based on current state and returns it

        Args:
            client_id (str): unique client id

        Returns:
            Task: Task object or None if no tasks available
        """
        with self.lock:
            if self.clients[client_id][2]:
                return self.tasks[self.clients[client_id][2]]
            elif self.state['pending_prime']:
                task_type = 0  # Prime task
                task_files = [self.state['pending_prime'].pop(0)]
            elif len(self.state['pending_de_duplication']) > 1:
                task_type = 1  # De-duplication task
                task_files = [self.state['pending_de_duplication'].pop(
                    0), self.state['pending_de_duplication'].pop(0)]
            elif len(self.state['pending_de_duplication']) == 1:
                if (self.check_processing_complete()):
                    self.result_filename = self.state['pending_de_duplication'][0]
                    print(
                        f"[Coordinator {self.server_id}] Processing complete. Final result file: {self.result_filename}")
                return None
            else:
                return None

            # In progress empty becuase not assigned yet. Only two current tasks should be there in the in-progress state
            if (self.state['pending_de_duplication'] == [] and
                self.state['in_progress_de_duplication'] == [] and
                self.state['pending_prime'] == [] and
                    self.state['in_progress_prime'] == []):
                final_task = True
            else:
                final_task = False

            # Creating Task object
            task = Task(client_id, task_type, task_files,
                        final_task=final_task)
            # Adding task to tasks dictionary
            self.tasks[task.task_id] = task
            return task

    def assign_task(self, client_id: str):
        """Create a task object and assigns it to a client, and updates the state

        Args:
            client_id (str): unique client id

        Returns:
            Task: Returns assigned Task object or None if no tasks available
        """
        with self.lock:
            # Create new task
            task = self.create_task(client_id)
            if task:
                print(
                    f"[Coordinator {self.server_id}] Assigning task {task.task_id} to client {client_id}")
                # Update assigned task ID in the clients dictionary
                self.clients[client_id][2] = task.task_id
                # Update state to reflect in-progress task
                if task.task_type == 0:
                    self.state['in_progress_prime'].append(task.task_files[0])
                else:
                    self.state['in_progress_de_duplication'].extend(
                        task.task_files)
                return task
            print(
                f"[Coordinator {self.server_id}] No tasks available to assign to client {client_id}")
            return None

    def complete_task(self, task_id, client_id, new_file: str):
        """Responds to client task complete, updates the state based on completed task

        Args:
            task_id (_type_): unique task id
            client_id (_type_): unique client id
            new_file (str): name of the new file returned by the client
        """
        with self.lock:
            task = self.tasks.get(int(task_id))
            if not task:
                print(
                    f"[Coordinator {self.server_id}] Task {task_id} not found.")
                return

            print(
                f"[Coordinator {self.server_id}] Completing task {task_id} from client {client_id}")
            # Update state based on task type
            if task.task_type == 0:
                # For prime task
                if task.task_files[0] not in self.state['in_progress_prime']:
                    print(
                        f"[Coordinator {self.server_id}] Task {task_id} already completed or not in progress.")
                else:
                    self.state['in_progress_prime'].remove(
                        task.task_files[0])  # Remove from in-progress
                    self.state['completed_prime'].append(
                        task.task_files[0])  # Add to completed

            else:
                # For de-duplication task
                if (task.task_files[0] not in self.state['in_progress_de_duplication'] or
                        task.task_files[1] not in self.state['in_progress_de_duplication'] or
                        task.task_files[0] in self.state['completed_de_duplication'] or
                        task.task_files[1] in self.state['completed_de_duplication']):
                    print(
                        f"[Coordinator {self.server_id}] Task {task_id} already completed or not in progress.")
                else:
                    # Remove both files from in-progress
                    self.state['in_progress_de_duplication'].remove(
                        task.task_files[0])
                    self.state['in_progress_de_duplication'].remove(
                        task.task_files[1])
                    # Add both files to completed
                    self.state['completed_de_duplication'].extend(
                        [task.task_files[0], task.task_files[1]])

            if new_file not in self.state['pending_de_duplication']:
                # Add new returned file to pending de-duplication
                self.state['pending_de_duplication'].append(new_file)
            # Clear assigned task ID for the client
            self.clients[client_id][2] = 0

    def check_dead_clients_reassign_tasks(self):
        for client_id, (alive_status, last_heartbeat, assigned_task_id, snapshot_bool, snapshot_bool2) in self.clients.items():
            if ((time.time() - last_heartbeat > self.client_response_timeout) and alive_status):
                # Mark client as dead
                self.clients[client_id][0] = False
                # Log dead client
                now = time.time()
                last_time = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(last_heartbeat))
                now_time = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(now))
                print(
                    f"[Coordinator {self.server_id}] Client {client_id} is considered dead. Last heartbeat at {last_time} (ts={last_heartbeat}), current time {now_time} (ts={now})")
                # Reassign the task if there was an assigned task
                if assigned_task_id:
                    self.reassign_task(assigned_task_id)

    def reassign_task(self, task_id):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                print(
                    f"[Coordinator {self.server_id}] Task {task_id} not found for reassignment.")
                return
            print(
                f"[Coordinator {self.server_id}] Reassigning task {task_id} back to pending state.")
            # Update state based on task type
            if task.task_type == 0:
                # For prime task
                if task.task_files[0] in self.state['in_progress_prime']:

                    self.state['in_progress_prime'].remove(
                        task.task_files[0])  # Remove from in-progress
                    self.state['pending_prime'].append(
                        task.task_files[0])  # Add back to pending
            else:
                # For de-duplication task
                for file in task.task_files:
                    if file in self.state['in_progress_de_duplication']:
                        self.state['in_progress_de_duplication'].remove(
                            file)  # Remove from in-progress
                        self.state['pending_de_duplication'].append(
                            file)  # Add back to pending

    def update_heartbeat(self, client_id: str):
        with self.lock:
            if (client_id in self.clients):
                # Update last heartbeat time
                self.clients[client_id][1] = time.time()
                self.clients[client_id][0] = True  # Mark as alive
                # print(
                #     f"[Coordinator {self.server_id}] Heartbeat received from client {client_id}")

    def check_primary(self):
        with self.lock:
            return self.primary_server
    # RPC Methods

    def RequestTask(self, request, context):
        if not self.check_primary():
            return
        client_id = request.client_id
        print(
            f"[Coordinator {self.server_id}] Client {client_id} is requesting a task.")
        self.register_client(client_id)
        task = self.assign_task(client_id)
        # No task available
        if not task:
            # Processing complete
            if self.check_processing_complete():
                return coordinator_pb2.TaskResponse(has_task=False, client_task_status=coordinator_pb2.ClientTaskStatus.COMPLETED)
            # Processing not complete, but no tasks available
            return coordinator_pb2.TaskResponse(has_task=False, client_task_status=coordinator_pb2.ClientTaskStatus.WAIT)

        if task.task_type == 0:  # FIND_PRIMES
            return coordinator_pb2.TaskResponse(
                has_task=True,
                task_type=coordinator_pb2.TaskType.FIND_PRIMES,
                filename=task.task_files[0][0],
                start_index=task.task_files[0][1],
                end_index=task.task_files[0][2],
                task_id=str(task.task_id),
                last_task=task.final_task
            )
        elif task.task_type == 1:  # MERGE
            return coordinator_pb2.TaskResponse(
                has_task=True,
                task_type=coordinator_pb2.TaskType.MERGE,
                task_id=str(task.task_id),
                file1=task.task_files[0],
                file2=task.task_files[1],
                last_task=task.final_task
            )

        # Just in case lol
        return coordinator_pb2.TaskResponse(has_task=False)

    def UpdateTaskStatus(self, request, context):
        if not self.check_primary():
            return
        client_id = request.client_id
        task_id = request.task_id
        output_filename = request.output_filename

        self.complete_task(task_id, client_id, output_filename)

        return coordinator_pb2.TaskStatusResponse(acknowledged=True)

    def SendHeartbeat(self, request, context):
        if not self.check_primary():
            return
        client_id = request.client_id
        self.update_heartbeat(client_id)
        with self.lock:
            trigger_snapshot = self.clients[client_id][3]
            snapshot_marker = self.clients[client_id][4]
            if trigger_snapshot:
                self.clients[client_id][3] = False  # Reset snapshot bool
                # Start snapshot channel recording
                self.clients[client_id][4] = True

            if snapshot_marker:
                # Reset snapshot channel recording
                self.clients[client_id][4] = False

        return coordinator_pb2.HeartbeatResponse(acknowledged=True, trigger_snapshot=trigger_snapshot, snapshot_marker=snapshot_marker)

    # RPC Methods End

    # Functions for snapshot
    def self_snapshot(self):
        with self.lock:
            state = self.get_state()
            self.snapshot = {
                "timestamp": time.time(),
                "coordinator": state,
                "clients": {}
            }

    def take_snapshot(self):
        with self.lock:
            # print(f"[Coordinator {self.server_id}] self.last_snapshot_time: {self.last_snapshot_time} server_uptime: {self.server_uptime} snapshot_interval: {self.snapshot_interval} interval check: {int(self.last_snapshot_time - self.server_uptime)}")

            if int(self.server_uptime - self.last_snapshot_time) < self.snapshot_interval:
                return
            print(
                f"[Coordinator {self.server_id}] Taking snapshot")
            self.last_snapshot_time = self.server_uptime
            self.self_snapshot()
            self.snapshot_tracker = {}
            for client_id in self.clients.keys():
                # If client is alive and snapbool is false
                if self.clients[client_id][0] and not (self.clients[client_id][3]):
                    # Set snapbool to true
                    self.clients[client_id][3] = True
                    # Snapshot request will be sent during next heartbeat
                    self.snapshot_tracker[client_id] = False
            snapshot_start_time = time.time()
            snapshot_timeout = 30  # seconds
            current_time = time.time()
            # wait for all clients to send their snapshot
        # Run the rest of the snapshot process in a separate thread to avoid blocking
        snapshot_thread = threading.Thread(
            target=self.wait_and_finalize_snapshot,
            args=(snapshot_start_time, snapshot_timeout)
        )
        snapshot_thread.start()

    def wait_and_finalize_snapshot(self, snapshot_start_time, snapshot_timeout):
        """
        Waits for client snapshots, saves the complete snapshot to disk,
        and uploads it to the file server. This method is intended to be
        run in a separate thread.
        """
        print(
            f"[Coordinator {self.server_id}] Waiting for client snapshots...")
        while not all(self.snapshot_tracker.values()):
            if ((time.time() - snapshot_start_time) > snapshot_timeout):
                print(
                    f"[Coordinator {self.server_id}] Snapshot timed out. Proceeding with available data.")
                break
            time.sleep(1)

        self.save_snapshot_to_disk()
        print(
            f"[Coordinator {self.server_id}] Uploading snapshot to file server...")
        self.upload_snapshot_to_file_server()
        print(f"[Coordinator {self.server_id}] Snapshot process completed.")

    def add_client_snapshot(self, client_id: str, snapshot_data: dict):
        with self.lock:
            if client_id in self.clients:
                self.snapshot["clients"][client_id] = snapshot_data

    def save_snapshot_to_disk(self):
        with self.lock:
            snapshot_filename = "snapshot.json"
            try:
                with open(snapshot_filename, 'w') as f:
                    json.dump(self.snapshot, f, indent=4)
                print(
                    f"[Coordinator {self.server_id}] Snapshot saved to {snapshot_filename}")
            except Exception as e:
                print(
                    f"[Coordinator {self.server_id}] Failed to save snapshot to disk: {e}")

    def upload_snapshot_to_file_server(self):
        with self.lock:

            return_value = False

            for fs_address in self.coordinator_addresses:
                print(
                    f"[Coordinator {self.server_id}] Uploading snapshot to file server at {fs_address}")
                try:
                    fileserver_channel = grpc.insecure_channel(fs_address)
                    fileserver_stub = fileserver_pb2_grpc.FileServerStub(
                        fileserver_channel)

                    chunk_iterator = self.stream_upload(
                        Path("./snapshot.json"), "snapshot.json")

                    response = fileserver_stub.UploadFile(
                        chunk_iterator, timeout=60.0, wait_for_ready=True)

                    if response.success:
                        print(
                            f"[Coordinator {self.server_id}] Successfully uploaded snapshot to {fs_address}")
                        return_value = True
                        break

                    # Server may redirect us to the actual primary
                    redirect_address = response.new_primary_fileserver

                    if redirect_address:
                        print(
                            f"[Coordinator {self.server_id}] Redirected to primary file server at {redirect_address}")
                        fileserver_channel = grpc.insecure_channel(
                            redirect_address)
                        fileserver_stub = fileserver_pb2_grpc.FileServerStub(
                            fileserver_channel)

                        chunk_iterator = self.stream_upload(
                            Path("./snapshot.json"), "snapshot.json")

                        response = fileserver_stub.UploadFile(
                            chunk_iterator, timeout=60.0, wait_for_ready=True)

                        if response.success:
                            print(
                                f"[Coordinator {self.server_id}] Successfully uploaded snapshot to primary file server at {redirect_address}")
                            return_value = True
                            break
                        else:
                            print(
                                f"[Coordinator {self.server_id}] Failed to upload snapshot to primary file server at {redirect_address}")
                            return_value = False

                except grpc.RpcError as e:
                    print(
                        f"[Coordinator {self.server_id}] Failed to upload snapshot to file server at {fs_address}: {e}")
            try:
                if fileserver_channel is not None:
                    fileserver_channel.close()
            except Exception as e:
                print(
                    f"[{self.server_id}] Failed to close connection to the file server")

            return return_value

    def stream_upload(self, filepath: Path, remote_filename: str, chunk_size: int = 65536):
        """Read local file and generate chunks for upload to file server."""
        file_size = filepath.stat().st_size

        # Compute checksum
        checksum = self.compute_checksum(filepath)

        # Send metadata first
        metadata = fileserver_pb2.FileMetadata(
            filename=remote_filename,
            file_size=file_size,
            checksum=checksum,
        )
        yield fileserver_pb2.FileChunk(metadata=metadata)

        # Then send file data in chunks
        with open(filepath, "rb") as f:
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break
                yield fileserver_pb2.FileChunk(chunk_data=chunk_data)

    def compute_checksum(self, filepath: Path, chunk_size: int = 65536) -> str:
        """Compute SHA-256 checksum of file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sha256.update(chunk)
        return sha256.hexdigest()

    def get_gloabl_snapshot_from_fs(self):
        """Returns the latest snapshot of the coordinator server

        Returns:
            dict: Dictionary containing the snapshot data or None if no snapshot available
        """

        for fs_address in self.coordinator_addresses:
            print(
                f"[Coordinator {self.server_id}] Retrieving snapshot from file server at {fs_address}")
            try:
                fileserver_channel = grpc.insecure_channel(fs_address)
                fileserver_stub = fileserver_pb2_grpc.FileServerStub(
                    fileserver_channel)

                request = fileserver_pb2.FetchRequest(
                    filename="snapshot.json")
                chunk_stream = fileserver_stub.FetchFile(request)

                # Save to local cache
                cached_path = Path("./retrieved_snapshot.json")
                self.stream_fetch(chunk_stream, cached_path)

            except grpc.RpcError as e:
                print(
                    f"[Coordinator {self.server_id}] Failed to retrieve snapshot from file server at {fs_address}")

            try:
                if fileserver_channel is not None:
                    fileserver_channel.close()
            except Exception as e:
                print(
                    f"[{self.client_id}] Failed to close connection to the file server")

            try:
                with open(cached_path, 'r') as f:
                    snapshot_data = json.load(f)
                    if snapshot_data.get("coordinator") != None and snapshot_data.get("coordinator") != {}:

                        print(
                            f"[Coordinator {self.server_id}] Successfully retrieved snapshot from file server at {fs_address}")
                        return snapshot_data

            except Exception as e:
                print(
                    f"[Coordinator {self.server_id}] Failed to load retrieved snapshot from file server at {fs_address}: {e}")

        return None

    def stream_fetch(self, chunk_stream, output_path: Path) -> str:
        """Receive chunks from file server and write atomically."""
        tmp_path = Path(str(output_path) + ".tmp")

        try:
            with open(tmp_path, "wb") as f:
                for chunk in chunk_stream:
                    if chunk.HasField("metadata"):
                        server_checksum = chunk.metadata.checksum
                    elif chunk.HasField("chunk_data"):
                        f.write(chunk.chunk_data)

            tmp_path.replace(output_path)

        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            raise e

    # RPC for snapshot

    def SubmitSnapshot(self, request, context):
        if not self.check_primary():
            return
        client_id = request.client_id
        snapshot_json = request.snapshot_json
        snapshot_data = json.loads(snapshot_json)
        self.add_client_snapshot(client_id, snapshot_data)
        with self.lock:
            if client_id in self.snapshot_tracker:
                self.snapshot_tracker[client_id] = True
        return coordinator_pb2.SnapshotSubmissionResponse(acknowledged=True)

    # RPC for snapshot  End

    # Functions for replica-primary setup

    def get_state(self) -> dict:
        """Returns the whole state of the coordinator server as a dictionary

        Returns:
            dict: Dictionary containing the all state data of the coordinator
        """
        with self.lock:
            # Convert Task objects to a serializable format (dictionaries)
            serializable_tasks = {
                task_id: task.__dict__ for task_id, task in self.tasks.items()
            }

            # Prepare the state data for serialization
            state_data = {
                "state": self.state,
                "clients": self.clients,
                "tasks": serializable_tasks,
                "task_id_static": Task.task_id_static,
                "result_filename": self.result_filename,
                "processing_complete": self.processing_complete,
            }
        return state_data

    def restore_state(self, state_data: dict):
        """Restores the whole state of the coordinator server using the input dictionary

        Args:
            state_data (dict): Dictionary containing the all state data of the coordinator
        """
        with self.lock:
            # Restore state
            self.state = state_data["state"]
            self.clients = state_data["clients"]
            self.tasks = {}
            for task_id, task_dict in state_data["tasks"].items():
                task = Task(
                    task_dict["client_id"],
                    task_dict["task_type"],
                    task_dict["task_files"],
                    task_dict["task_status"],
                    task_dict["task_id"],
                )
                self.tasks[int(task_id)] = task
            Task.set_global_taskID(int(state_data["task_id_static"]))
            self.result_filename = state_data["result_filename"]
            self.processing_complete = bool(
                state_data["processing_complete"])

    def connect_to_primary(self):
        """Tries to connect to primary, if cannot connect assumes to be primary
        """
        with self.lock:
            if self.replica_server_address:
                # Try to connect to replica server to determine if primary or secondary
                try:
                    # Establish a connection to the replica
                    self.replica_channel = grpc.insecure_channel(
                        self.replica_server_address)
                    # Check if the channel is ready
                    grpc.channel_ready_future(
                        self.replica_channel).result(timeout=2)
                    self.replica_stub = coordinator_pb2_grpc.CoordinatorInternalStub(
                        self.replica_channel)
                    print(
                        f"[Coordinator {self.server_id}] Successfully connected to replica server.")
                    self.primary_server = False  # This server is secondary
                except grpc.FutureTimeoutError:
                    # If cannot connect to replica, this is primary
                    print(
                        f"[Coordinator {self.server_id}] Cannot connect to replica server at {self.replica_server_address}.")
                    print(
                        f"[Coordinator {self.server_id}] This server is primary.")
                    self.primary_server = True  # This server is primary
                except Exception as e:
                    # Unexpected error
                    print(
                        f"[Coordinator {self.server_id}] Failed to connect to replica server: {e}")
                    sys.exit(1)

    def send_internal_heartbeat(self):
        with self.lock:
            if not self.primary_server:
                try:
                    request = coordinator_pb2.InternalHeartbeatRequest()
                    response = self.replica_stub.InternalHeartbeat(
                        request, timeout=1)
                    if response.acknowledged:
                        print(
                            f"[Coordinator {self.server_id}]  Primary acknowledged heartbeat.")
                        self.last_primary_heartbeat = time.time()
                except grpc.RpcError as e:
                    print(
                        f"[Coordinator {self.server_id}] Failed to send heartbeat to primary: {e}")
                    print(
                        f"[Coordinator {self.server_id}] Assuming primary is down. Promoting to primary.")
                    self.primary_server = True
                    self.replica_channel.close()
                    self.replica_stub = None
                    self.replica_server_address = None

    def sync_state_to_primary(self):
        with self.lock:
            if not self.primary_server:
                try:
                    request = coordinator_pb2.GetFullStateRequest()
                    response = self.replica_stub.GetFullState(
                        request, timeout=2)
                    state_json = response.state_json.decode("utf-8")
                    state_data = json.loads(state_json)

                    self.restore_state(state_data)

                    print(
                        f"[Coordinator {self.server_id}] State synchronized from primary server.")
                except grpc.RpcError as e:
                    print(
                        f"[Coordinator {self.server_id}] Failed to sync state from primary: {e}")

    def secondary_server_loop(self):
        while not self.check_primary():
            self.send_internal_heartbeat()
            self.sync_state_to_primary()
            time.sleep(1)

    # RPC Methods for replica-primary setup

    def InternalHeartbeat(self, request, context):
        if not self.check_primary():
            return
        return coordinator_pb2.InternalHeartbeatResponse(acknowledged=True)

    def GetFullState(self, request, context):
        # Return the full state of the coordinator server as json
        with self.lock:
            if self.primary_server:
                state_data = self.get_state()

                # Serialize the state data to a JSON string, then encode to bytes
                state_json_bytes = json.dumps(state_data).encode("utf-8")

                # Return the response
                return coordinator_pb2.FullStateResponse(state_json=state_json_bytes)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        coordinator_pb2_grpc.add_CoordinatorServicer_to_server(
            self, server)
        coordinator_pb2_grpc.add_CoordinatorInternalServicer_to_server(
            self, server)
        server.add_insecure_port(f"{self.address}:{self.port}")
        server.start()
        print(
            f"[Coordinator {self.server_id}] Coordinator Server started at {self.address}:{self.port}")
        if not self.primary_server:
            self.secondary_server_loop()
        try:
            while True:
                time.sleep(1)
                self.server_uptime = time.time()
                self.check_dead_clients_reassign_tasks()
                self.take_snapshot()
        except KeyboardInterrupt:
            print(
                f"[Coordinator {self.server_id}] Shutting down Coordinator Server...")
            server.stop(0)


class Task:

    task_id_static = 0  # Starts from 0, increments with each new task

    def __init__(self, client_id: str, task_type: str, task_files: list, task_status: int = 0, task_id: int = 0, final_task: bool = False):
        self.task_id = Task.task_id_static + 1 if task_id == 0 else task_id
        self.client_id = client_id
        self.task_type = task_type  # 0: prime, 1: de-duplication
        self.task_files = task_files
        self.task_status = task_status  # 0: in-progress, 1: completed
        self.final_task = final_task  # Is this the final task
        Task.task_id_static += 1

    def set_global_taskID(task_id: int):
        Task.task_id_static = task_id


def main():
    parser = argparse.ArgumentParser(description="Coordinator server CLI")
    parser.add_argument("--address", "-a",
                        default="localhost", help="Server address")
    parser.add_argument("--port", "-p", type=int,
                        default=50051, help="Server port")
    parser.add_argument("--file-list", "-f",
                        default="file_list.csv", help="Path to the file list")
    parser.add_argument("--timeout", "-t", type=int, default=30,
                        help="Client response timeout (seconds)")
    parser.add_argument("--server-id", "-s", type=int,
                        default=1, help="Unique server ID")
    parser.add_argument("--replica-address", "-r",
                        default=None, help="Replica server address (if any)")
    parser.add_argument("--snapshot-interval", "-i", type=int,
                        default=60, help="Snapshot interval in seconds")
    parser.add_argument("--fileserver-addresses", "-fs", nargs='+',
                        default=[], help="List of file server addresses")
    args = parser.parse_args()

    file_path = Path(args.file_list)
    if not file_path.exists():
        print(
            f"[Coordinator {args.server_id}] File list not found: {file_path}")
        sys.exit(1)

    coordinator_server = CoordinatorServer(
        address=args.address,
        port=args.port,
        file_list_path=str(file_path),
        client_response_timeout=args.timeout,
        server_id=args.server_id,
        replica_server_address=args.replica_address,
        snapshot_interval=args.snapshot_interval,
        fileserver_addresses=args.fileserver_addresses,
    )
    coordinator_server.initialize()
    coordinator_server.serve()


if __name__ == "__main__":
    main()
