"""File discovery and staging utilities."""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def _get_search_paths() -> List[Path]:
    """Get common directories where files might be located."""
    home = Path.home()
    cwd = Path.cwd()

    paths = [
        cwd,
        cwd / "data",
        cwd / "result_export",
        cwd / "uploads",
        home,
        home / "Downloads",
        home / "Documents",
        home / "Desktop",
    ]

    return [p for p in paths if p.exists()]


def find_files(
    pattern: str = "*.csv",
    search_name: str = "",
    max_results: int = 20,
) -> Dict[str, Any]:
    """Search for files across common directories.

    Args:
        pattern: Glob pattern to match (e.g. '*.csv', '*.xlsx').
        search_name: Optional filename substring to filter (case-insensitive).
        max_results: Maximum number of results.

    Returns:
        Dict with found files, paths, sizes, and modification times.
    """
    search_paths = _get_search_paths()
    found_files: List[Dict[str, Any]] = []
    seen_paths: set = set()

    for search_dir in search_paths:
        try:
            for file_path in search_dir.rglob(pattern):
                if len(found_files) >= max_results:
                    break

                abs_path = str(file_path.resolve())
                if abs_path in seen_paths:
                    continue
                seen_paths.add(abs_path)

                if search_name and search_name.lower() not in file_path.name.lower():
                    continue

                if len(file_path.parts) > 15:
                    continue

                try:
                    stat = file_path.stat()
                    found_files.append({
                        "path": str(file_path),
                        "name": file_path.name,
                        "size_kb": round(stat.st_size / 1024, 2),
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "directory": str(file_path.parent),
                    })
                except (OSError, PermissionError):
                    continue
        except (OSError, PermissionError):
            continue

    found_files.sort(key=lambda x: x["modified"], reverse=True)
    found_files = found_files[:max_results]

    return {
        "pattern": pattern,
        "search_name": search_name or "(none)",
        "directories_searched": [str(p) for p in search_paths],
        "files_found": len(found_files),
        "files": found_files,
    }


def stage_file(
    source_path: str,
    dest_dir: str,
    new_name: str = "",
) -> Dict[str, Any]:
    """Copy a file to a destination directory.

    Args:
        source_path: Full path to the source file.
        dest_dir: Destination directory path.
        new_name: Optional new filename.

    Returns:
        Dict with source, destination, and file info.
    """
    source = Path(source_path)
    if not source.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    dest_name = new_name if new_name else source.name
    dest_path_dir = Path(dest_dir)
    dest_path_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_path_dir / dest_name

    if dest_path.exists():
        stem = dest_path.stem
        suffix = dest_path.suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_path = dest_path_dir / f"{stem}_{timestamp}{suffix}"

    shutil.copy2(source, dest_path)
    stat = dest_path.stat()

    return {
        "status": "success",
        "source": str(source),
        "destination": str(dest_path),
        "size_kb": round(stat.st_size / 1024, 2),
    }
