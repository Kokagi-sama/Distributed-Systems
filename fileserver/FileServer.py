import argparse
import grpc
from concurrent import futures
import threading
import sys
import os
import time
import random
import hashlib
import shutil
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator

# --- PATH SETUP ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from generated import fileserver_pb2
    from generated import fileserver_pb2_grpc
except ImportError:
    print("\nERROR: Could not import generated proto files.")
    sys.exit(1)

# --- RAFT STATES ---
STATE_FOLLOWER: str = "FOLLOWER"
STATE_CANDIDATE: str = "CANDIDATE"
STATE_LEADER: str = "LEADER"

class FileServer(fileserver_pb2_grpc.FileServerServicer):

    def __init__(
        self, 
        server_id: str, 
        peers: List[str], 
        port: int, 
        input_storage_root: str = "fileserver_storage",
        output_storage_root: str = "fileserver_output",
        snapshot_storage_root: str = "fileserver_snapshots"
    ) -> None:
        self.lock: threading.Lock = threading.Lock()
        self.server_id: str = server_id
        self.peers: List[str] = peers
        self.port: int = port

        # --- PERSISTENT NETWORK CHANNELS ---
        self.peer_stubs: Dict[str, fileserver_pb2_grpc.FileServerStub] = {}
        for peer in self.peers:
            channel = grpc.insecure_channel(peer)
            self.peer_stubs[peer] = fileserver_pb2_grpc.FileServerStub(channel)

        # --- STORAGE SETUP ---
        self.machine_root: Path = Path(self.server_id)
        self.storage_dir: Path = self.machine_root / input_storage_root
        self.output_dir: Path = self.machine_root / output_storage_root
        self.snapshot_dir: Path = self.machine_root / snapshot_storage_root
        self.temp_dir: Path = self.machine_root / "tmp"
        for p in [self.storage_dir, self.output_dir, self.snapshot_dir, self.temp_dir]:
            p.mkdir(parents=True, exist_ok=True)

        # --- IN-MEMORY STATE ---
        self.files: Dict[str, Dict[str, Any]] = {}
        self.max_version: int = 0 

        # --- RAFT STATE ---
        self.state: str = STATE_FOLLOWER
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = random.uniform(3.0, 6.0)

        # --- STARTUP ---
        self._recover_state_from_disk()
        self.running: bool = True
        
        threading.Thread(target=self._consensus_election_monitor, daemon=True).start()
        threading.Thread(target=self._consensus_heartbeat_emitter, daemon=True).start()

        print(f"[{self.server_id}] ONLINE | Port: {self.port} | Max Version: {self.max_version}")
        print(f"[{self.server_id}] Input Dir: {self.storage_dir}")
        print(f"[{self.server_id}] Output Dir: {self.output_dir}")
        print(f"[{self.server_id}] Snapshot Dir: {self.snapshot_dir}")


    # ========================================================================
    # HELPER FUNCTIONS (INTERNAL)
    # ========================================================================

    def _get_final_path(self, filename: str) -> Path:
        if filename.endswith("primes.txt") or filename.startswith("merged_"):
            return self.output_dir / filename
        elif filename.startswith("snapshot_"):
            return self.snapshot_dir / filename
        return self.storage_dir / filename

    def _calculate_file_checksum(self, file_path: Path) -> str:
        hasher = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(1024*1024), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError: return ""

    def _save_metadata(self, filename: str, metadata: Dict[str, Any]) -> None:
        data_path = self._get_final_path(filename)
        meta_path = data_path.parent / f"{data_path.name}.meta"
        with open(meta_path, 'w') as f:
            json.dump(metadata, f)

    def _recover_state_from_disk(self) -> None:
        if self.temp_dir.exists(): shutil.rmtree(self.temp_dir)
        self.temp_dir.mkdir()

        # --- 1. Scan for DYNAMIC files (Output and Snapshots) ---
        # These have .meta files and are versioned
        scan_dirs = [self.output_dir, self.snapshot_dir]
        for dir_path in scan_dirs:
            for entry in os.scandir(dir_path):
                if entry.is_file() and entry.name.endswith(".meta"):
                    try:
                        with open(entry.path, 'r') as f:
                            meta = json.load(f)
                        
                        filename = entry.name[:-5]
                        if (dir_path / filename).exists():
                            self.files[filename] = meta
                            if meta['version'] > self.max_version:
                                self.max_version = meta['version']
                    except Exception as e:
                        print(f"[{self.server_id}] Corrupt metadata for {entry.name}: {e}")
        
        # --- 2. Scan for STATIC input files (in self.storage_dir) ---
        # These are manually added, have no .meta, and are version 0
        try:
            for entry in os.scandir(self.storage_dir):
                # Find files, but ignore .meta files in case any exist
                if entry.is_file() and not entry.name.endswith(".meta"):
                    filename = entry.name
                    # Only add if not already in memory (e.g., if a file with
                    # the same name was somehow in the output dir)
                    if filename not in self.files:
                        try:
                            file_path = Path(entry.path)
                            file_size = entry.stat().st_size
                            # Note: This calculates checksum for all inputs at startup.
                            # This is correct for AFS but can be slow if files are huge.
                            file_checksum = self._calculate_file_checksum(file_path)
                            
                            self.files[filename] = {
                                "size": file_size,
                                "checksum": file_checksum,
                                "version": 0  # Static input files are version 0
                            }
                            print(f"[{self.server_id}] Discovered static input file: {filename}")
                        except Exception as e:
                            print(f"[{self.server_id}] Failed to load static file {filename}: {e}")
        except FileNotFoundError:
            print(f"[{self.server_id}] WARN: Input directory {self.storage_dir} not found during scan.")
        except Exception as e:
            print(f"[{self.server_id}] ERROR: Failed to scan input dir {self.storage_dir}: {e}")

    # ========================================================================
    # CLIENT-FACING RPCs
    # ========================================================================

    def UploadFile(
        self, request_iterator: Iterator[fileserver_pb2.FileChunk], context: grpc.ServicerContext
    ) -> fileserver_pb2.UploadResponse:
        try:
            first = next(request_iterator)
            if not first.HasField("metadata"): 
                return fileserver_pb2.UploadResponse(success=False, error_message="Missing metadata")
            
            if self.state != STATE_LEADER:
                 return fileserver_pb2.UploadResponse(
                        success=False, 
                        error_message="Not leader", 
                        new_primary_fileserver=self.leader_id or ""
                 )

            filename = first.metadata.filename
            temp_path = self.temp_dir / f"{filename}.tmp"
            hasher = hashlib.sha256()
            received_bytes = 0

            with open(temp_path, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("chunk_data"):
                        f.write(chunk.chunk_data)
                        hasher.update(chunk.chunk_data)
                        received_bytes += len(chunk.chunk_data)

            final_path = self._get_final_path(filename)
            os.replace(temp_path, final_path)
            final_csum = hasher.hexdigest()

            with self.lock:
                current_ver = self.files.get(filename, {}).get("version", 0)
                new_ver = current_ver + 1
                self.files[filename] = {"size": received_bytes, "checksum": final_csum, "version": new_ver}
                
                self._save_metadata(filename, self.files[filename])
                
                if new_ver > self.max_version: self.max_version = new_ver

            print(f"[{self.server_id}] UPLOAD COMMIT: {filename} to {final_path.parent} (v{new_ver})")
            
            threading.Thread(target=self._async_replication_manager, 
                             args=(filename, new_ver, final_csum, received_bytes, final_path), 
                             daemon=True).start()
            return fileserver_pb2.UploadResponse(success=True)
        except Exception as e:
             return fileserver_pb2.UploadResponse(success=False, error_message=str(e))

    def FetchFile(
        self, request: fileserver_pb2.FetchRequest, context: grpc.ServicerContext
    ) -> Iterator[fileserver_pb2.FileChunk]:
        filename = request.filename
        meta = None
        with self.lock:
            meta = self.files.get(filename)

        if not meta:
            # Check if it's a static file we missed on startup (fallback)
            # This is less efficient but makes it robust if files are added manually *while running*
            file_path_fallback = self.storage_dir / filename
            if file_path_fallback.exists():
                print(f"[{self.server_id}] WARN: {filename} not in memory, loading from static dir...")
                try:
                    file_size = file_path_fallback.stat().st_size
                    file_checksum = self._calculate_file_checksum(file_path_fallback)
                    meta = {"size": file_size, "checksum": file_checksum, "version": 0}
                    with self.lock:
                        self.files[filename] = meta # Cache it in memory now
                except Exception as e:
                    print(f"[{self.server_id}] ERROR: Failed fallback load of {filename}: {e}")
                    context.abort(grpc.StatusCode.NOT_FOUND, "File not found")
                    return
            
            # Handle special snapshot case
            elif "snapshot.json" in filename:
                print(f"[{self.server_id}] No {filename} found. Creating temp empty file to stream.")
                empty_content = b"{}"
                
                temp_path = self.temp_dir / f"temp_{filename}_{time.time_ns()}" 
                try:
                    with open(temp_path, 'wb') as f:
                        f.write(empty_content)
                    
                    empty_size = len(empty_content)
                    empty_checksum = hashlib.sha256(empty_content).hexdigest()

                    yield fileserver_pb2.FileChunk(metadata=fileserver_pb2.FileMetadata(
                        filename=filename, 
                        file_size=empty_size,
                        checksum=empty_checksum, 
                        version=0
                    ))
                    
                    with open(temp_path, 'rb') as f:
                        yield fileserver_pb2.FileChunk(chunk_data=f.read())
                    
                finally:
                    if temp_path.exists():
                        try:
                            temp_path.unlink()
                        except Exception as e:
                            print(f"[{self.server_id}] WARN: Failed to delete temp file {temp_path}: {e}")
                
                return
            else:
                # If it's not a snapshot and not a static file, it's truly not found
                context.abort(grpc.StatusCode.NOT_FOUND, "File not found")
                return
            
        current_meta = meta.copy()

        file_path = self._get_final_path(filename)
        if not file_path.exists():
             print(f"[{self.server_id}] DATA LOSS ERROR: Metadata for {filename} exists, but file not found at {file_path}")
             context.abort(grpc.StatusCode.DATA_LOSS, "File missing on disk despite metadata")
             return

        yield fileserver_pb2.FileChunk(metadata=fileserver_pb2.FileMetadata(
                filename=filename, file_size=current_meta["size"],
                checksum=current_meta["checksum"], version=current_meta["version"]))
        
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk: break
                yield fileserver_pb2.FileChunk(chunk_data=chunk)
                
    # ========================================================================
    # REPLICATION SERVICES
    # ========================================================================

    def ReplicateFile(
        self, request_iterator: Iterator[fileserver_pb2.ReplicateFileRequest], context: grpc.ServicerContext
    ) -> fileserver_pb2.ReplicateFileResponse:
        try:
            first = next(request_iterator)
            if not first.HasField("metadata"): 
                return fileserver_pb2.ReplicateFileResponse(error="No metadata")
            meta = first.metadata
            
            with self.lock:
                if self.files.get(meta.filename, {}).get("version", 0) >= meta.version:
                    return fileserver_pb2.ReplicateFileResponse(error="Already have version")

            temp_path = self.temp_dir / f"{meta.filename}.rep"
            hasher = hashlib.sha256()
            with open(temp_path, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("file_data"):
                        f.write(chunk.file_data)
                        hasher.update(chunk.file_data)

            if hasher.hexdigest() != meta.checksum:
                os.remove(temp_path)
                return fileserver_pb2.ReplicateFileResponse(error="Checksum mismatch")

            final_path = self._get_final_path(meta.filename)
            os.replace(temp_path, final_path)
            
            with self.lock:
                self.files[meta.filename] = {"size": meta.file_size, "checksum": meta.checksum, "version": meta.version}
                self._save_metadata(meta.filename, self.files[meta.filename])
                if meta.version > self.max_version: self.max_version = meta.version
            
            print(f"[{self.server_id}] REPLICATED: {meta.filename} to {final_path.parent} (v{meta.version})")
            return fileserver_pb2.ReplicateFileResponse(error="")
        except Exception as e: 
            return fileserver_pb2.ReplicateFileResponse(error=str(e))

    def _async_replication_manager(
        self, filename: str, version: int, checksum: str, size: int, file_path: Path
    ) -> None:
        for peer in self.peers:
             threading.Thread(target=self._perform_peer_replication, 
                              args=(peer, filename, version, checksum, size, file_path), daemon=True).start()

    def _perform_peer_replication(
        self, peer: str, filename: str, version: int, checksum: str, size: int, file_path: Path
    ) -> None:
        try:
            stub = self.peer_stubs.get(peer)
            if not stub: return
            def stream() -> Iterator[fileserver_pb2.ReplicateFileRequest]:
                yield fileserver_pb2.ReplicateFileRequest(metadata=fileserver_pb2.FileMetadata(
                        filename=filename, version=version, checksum=checksum, file_size=size))
                with open(file_path, 'rb') as f:
                    while True:
                        c = f.read(1024*1024)
                        if not c: break
                        yield fileserver_pb2.ReplicateFileRequest(file_data=c)
            stub.ReplicateFile(stream(), timeout=60)
        except Exception as e: 
            print(f"[{self.server_id}] Replication FAILED to {peer}: {e}")

    # ========================================================================
    # RAFT CONSENSUS (VOTING & HEARTBEATS)
    # ========================================================================

    def RequestVote(
        self, request: fileserver_pb2.VoteRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.VoteResponse:
        with self.lock:
            if request.term > self.current_term:
                print(f"[{self.server_id}] STEP DOWN: Higher term {request.term} in RequestVote")
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.voted_for = None

            if request.term < self.current_term:
                return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            if request.last_log_index < self.max_version:
                 print(f"[{self.server_id}] VOTE DENIED: Candidate {request.candidate_id} is stale")
                 return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            if self.voted_for is None or self.voted_for == request.candidate_id:
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                print(f"[{self.server_id}] VOTE GRANTED to {request.candidate_id} (Term {self.current_term})")
                return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=True)
            
            return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=False)

    def Heartbeat(
        self, request: fileserver_pb2.HeartbeatRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.HeartbeatResponse:
        with self.lock:
            if request.term < self.current_term:
                return fileserver_pb2.HeartbeatResponse(term=self.current_term, success=False)

            if request.term >= self.current_term:
                if self.state == STATE_LEADER and request.leader_id != self.server_id:
                     print(f"[{self.server_id}] STEP DOWN: Discovered valid leader {request.leader_id}")
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.leader_id = request.leader_id
                self.last_heartbeat = time.time()
                self.voted_for = None

            return fileserver_pb2.HeartbeatResponse(term=self.current_term, success=True)

    def _trigger_election(self) -> None:
        with self.lock:
            self.state = STATE_CANDIDATE
            self.current_term += 1
            self.voted_for = self.server_id
            self.last_heartbeat = time.time()
            self.election_timeout = random.uniform(3.0, 6.0)
            term = self.current_term
            last_index = self.max_version
        
        print(f"[{self.server_id}] ELECTION STARTED (Term {term}, LastVer {last_index})")
        votes = 1
        
        def request_vote_worker(peer: str, stub: fileserver_pb2_grpc.FileServerStub) -> None:
            nonlocal votes
            try:
                resp = stub.RequestVote(
                    fileserver_pb2.VoteRequest(
                        term=term, candidate_id=self.server_id,
                        last_log_index=last_index, last_log_term=0
                    ), timeout=2.0)
                with self.lock:
                    if resp.term > self.current_term:
                        self.current_term = resp.term
                        self.state = STATE_FOLLOWER
                        self.voted_for = None
                        return
                    if resp.vote_granted and self.state == STATE_CANDIDATE and self.current_term == term:
                        votes += 1
            except Exception: pass

        with futures.ThreadPoolExecutor(max_workers=len(self.peers)) as ex:
            futures.wait([ex.submit(request_vote_worker, p, self.peer_stubs[p]) for p in self.peers], timeout=2.5)

        with self.lock:
            if self.state == STATE_CANDIDATE and self.current_term == term and votes > (len(self.peers) + 1) // 2:
                self.state = STATE_LEADER
                self.leader_id = self.server_id
                print(f"[{self.server_id}] *** ASSUMED LEADERSHIP (Term {term}) ***")
                threading.Thread(target=self._emit_heartbeats, daemon=True).start()

    def _emit_heartbeats(self) -> None:
        with self.lock: term = self.current_term
        for peer, stub in self.peer_stubs.items():
            try:
                resp = stub.Heartbeat(fileserver_pb2.HeartbeatRequest(term=term, leader_id=self.server_id), timeout=0.5)
                with self.lock:
                    if resp.term > self.current_term:
                         self.current_term = resp.term
                         self.state = STATE_FOLLOWER
                         return
            except Exception: pass

    def _consensus_election_monitor(self) -> None:
        while self.running:
            time.sleep(0.1)
            if self.state != STATE_LEADER and (time.time() - self.last_heartbeat > self.election_timeout):
                self._trigger_election()

    def _consensus_heartbeat_emitter(self) -> None:
        while self.running:
            time.sleep(1.0)
            if self.state == STATE_LEADER: self._emit_heartbeats()

# ========================================================================
# MAIN ENTRY POINT
# ========================================================================

def run_server(
    server_id: str, 
    port: int, 
    peers: List[str], 
    input_storage_dir: str,
    output_storage_dir: str,
    snapshot_storage_dir: str
) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    servicer = FileServer(server_id, peers, port, input_storage_dir, output_storage_dir, snapshot_storage_dir) 
    fileserver_pb2_grpc.add_FileServerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[{server_id}] FileServer started on port {port}")
    try: server.wait_for_termination()
    except KeyboardInterrupt: server.stop(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", type=str, default="")
    parser.add_argument("--input_storage", type=str, default="./fileserver_storage", help="Directory for input files")
    parser.add_argument("--output_storage", type=str, default="./fileserver_output", help="Directory for output/result files")
    parser.add_argument("--snapshot_storage", type=str, default="./fileserver_snapshots", help="Directory for snapshot files")
    
    args = parser.parse_args()
    
    run_server(
        args.id, 
        args.port, 
        [p for p in args.peers.split(",") if p.strip()], 
        args.input_storage,
        args.output_storage,
        args.snapshot_storage
    )