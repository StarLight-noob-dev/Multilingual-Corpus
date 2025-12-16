from pathlib import Path
from typing import ClassVar, Union, List


class ProjectRootFinder:

    ROOT_MARKERS: ClassVar[List[str]] = ['.git', 'pyproject.toml', 'setup.py']

    @classmethod
    def find_project_root(cls, start_path: Union[str, Path], max_recursion: int = 10) -> Path:
        """
        Finds the project root directory by looking for specific marker files or directories.

        Args:
            start_path (Union[str, Path]): The path to start searching from.
            max_recursion (int): Maximum number of parent directories to traverse.

        Returns:
            Path: The path to the project root directory (using pathlib.Path).
        """

        current_path = Path(start_path).resolve()

        for _ in range(max_recursion):
            # Check for multiple markers and the 'src' directory
            is_marker_present = any((current_path / marker).exists() for marker in cls.ROOT_MARKERS)
            is_src_present = (current_path / 'src').is_dir()

            if is_marker_present or is_src_present:
                return current_path

            parent_path = current_path.parent

            # Check if we have hit the filesystem root (e.g., 'C:\' or '/')
            if parent_path == current_path:
                break

            current_path = parent_path

        marker_list = ", ".join(cls.ROOT_MARKERS)
        raise FileNotFoundError(
            f"Could not locate project root. Looked up to {max_recursion} levels "
            f"for markers ({marker_list}) or 'src' directory "
            f"starting from: {start_path}"
        )
