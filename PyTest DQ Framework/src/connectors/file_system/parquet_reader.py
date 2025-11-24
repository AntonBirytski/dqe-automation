import os
import pandas as pd
from typing import List, Optional


class ParquetReader:
    """
    Utility class to load Parquet files inside pytest tests.
    Supports single files or directories containing multiple parquet parts.
    """
    def __init__(self, base_path: Optional[str] = None):
        self.base_path = base_path

    def _resolve_path(self, path: str) -> str:
        """
        Resolve path relative to base_path if provided.
        """
        if self.base_path:
            return os.path.join(self.base_path, path)
        return path

    def read(self, path: str) -> pd.DataFrame:
        """
        Read a parquet file or a folder of parquet files into a Dataframe.
        """
        resolved_path = self._resolve_path(path)

        if not os.path.exists(resolved_path):
            raise FileNotFoundError(f"Path does not exist: {resolved_path}")

        return pd.read_parquet(resolved_path)

    def read_partitioned(self, path: str) -> pd.DataFrame:
        """
        Recursively read ALL parquet files inside a folder and its subfolders.
        Useful for deeply partitioned or broken parquet structures.
        """
        resolved_path = self._resolve_path(path)

        if not os.path.exists(resolved_path):
            raise FileNotFoundError(f"Path does not exist: {resolved_path}")

        parquet_files: List[str] = []

        # Recursively walk through folders
        for root, _, files in os.walk(resolved_path):
            for f in files:
                if f.lower().endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))

        if not parquet_files:
            raise ValueError(f"No parquet files found in: {resolved_path}")

        # Load and concatenate all parquet parts
        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)

    def load(self, path: str, recursive: bool = False) -> pd.DataFrame:
        """
        Unified loader.
        If recursive=True: loads from subfolders (read_partitioned).
        If recursive=False: loads single file or simple parquet folder (read).
        """
        if recursive:
            return self.read_partitioned(path)
        return self.read(path)
