# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building Kart

Kart uses CMake for building. **Python 3.11 is required** for compatibility with CI builds.

**Recommended approach (using VCPKG):**
```bash
# Configure with VCPKG dependency management
cmake -B build -S . -DPython3_EXECUTABLE=/path/to/python3.11 -DUSE_VCPKG=ON

# Build development version
cmake --build build

# Test the build
./build/kart --version
```

**Alternative (using CI vendor archives for faster builds):**
```bash
# Download vendor-{os}-{arch}-py3.11.zip from recent CI builds
cmake -B build -S . -DPython3_EXECUTABLE=/path/to/python3.11 -DVENDOR_ARCHIVE=/path/to/vendor-archive.zip -DUSE_VCPKG=OFF
cmake --build build
```

**Production build:**
```bash
# Build bundled binary and install
cmake --build build --target bundle
cmake --install build
```

### Testing

```bash
# Run full test suite
./build/venv/bin/pytest -v

# Run tests excluding slow ones
./build/venv/bin/pytest -m "not slow"

# Run specific test categories
./build/venv/bin/pytest -m "mssql"    # SQL Server tests
./build/venv/bin/pytest -m "mysql"   # MySQL tests
./build/venv/bin/pytest -m "e2e"     # End-to-end tests
```

### Code Quality

```bash
# Linting (via Ruff)
ruff check kart/

# Type checking
mypy kart/

# Format code
ruff format kart/
```

**Note**: Pre-commit hooks automatically run Ruff, MyPy, and CMake formatting tools.

## Architecture Overview

Kart is a **distributed version control system for geospatial data** that extends Git's object model to handle tabular, point cloud, and raster datasets.

### Core Components

**Repository Management** (`kart/repo.py`):
- `KartRepo`: Extends pygit2.Repository with geospatial-aware operations
- Supports both "bare-style" (legacy) and "tidy-style" repositories
- Built-in Git LFS integration and spatial filtering support

**Dataset Abstraction** (`kart/base_dataset.py`):
- `BaseDataset`: Abstract base for all dataset types (table, point-cloud, raster)
- Immutable view pattern - datasets are views of git trees
- Common diff/merge interface across all data types

**Multi-Format Support**:
- **Tabular** (`kart/tabular/`): Database-backed (GPKG, PostGIS, MySQL, SQL Server)
- **Point Cloud** (`kart/point_cloud/`): Tile-based LAZ/LAS with PDAL integration
- **Raster** (`kart/raster/`): Tile-based GeoTIFF with GDAL integration

**Working Copy System** (`kart/working_copy.py`):
- Multi-part architecture supporting both database and file-based backends
- Conflict detection and resolution for concurrent edits
- State tracking against repository HEAD

**CLI Framework** (`kart/cli.py`):
- Click-based modular command system
- Direct Git command pass-through for compatible operations
- Helper process architecture for environment isolation

### Key Design Patterns

- **Plugin Architecture**: Different strategies for tabular vs. tile-based data
- **Repository Pattern**: Clean separation between git operations and geospatial handling
- **Factory Pattern**: Dataset instantiation based on directory structure
- **Streaming Processing**: Incremental handling of large datasets

### Data Storage Strategy

- All geospatial data stored as git objects (blobs/trees/commits)
- Features/tiles organized in balanced hierarchical trees
- Metadata stored separately from data for efficient operations
- Automatic Git LFS integration for large files
- Schema versioning with V2/V3 formats for tables

### Important File Locations

- **Main entry**: `kart/cli.py`
- **Core repository logic**: `kart/repo.py`, `kart/core.py`
- **Dataset base classes**: `kart/base_dataset.py`
- **Format-specific implementations**: `kart/tabular/`, `kart/point_cloud/`, `kart/raster/`
- **Working copy backends**: `kart/tabular/working_copy/`
- **Build configuration**: `CMakeLists.txt`, `pyproject.toml`
- **Test configuration**: `pytest.ini`

When working with this codebase, understand that Kart bridges the gap between Git's file-based version control and the specialized needs of geospatial data management, requiring careful handling of both git operations and geospatial data formats.
