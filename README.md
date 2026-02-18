# deriva-imaging-pipeline

Tools and configuration for the DERIVA imaging pipeline.

## Prerequisites

- **uv** (Python package manager): https://docs.astral.sh/uv/getting-started/installation/
- **Java 17+** (required by bioformats)
  - macOS: `brew install openjdk@17`
  - RHEL/Fedora: `dnf install java-17-openjdk`
  - Ubuntu/Debian: `apt install openjdk-17-jdk`
- **libvips** (required by pyvips)
  - macOS: `brew install vips`
  - RHEL/Fedora: `dnf install vips vips-devel`
  - Ubuntu/Debian: `apt install libvips-dev`

## Installation

```bash
git clone https://github.com/informatics-isi-edu/deriva-imaging-pipeline.git
cd deriva-imaging-pipeline

# Create and activate virtual environment
uv venv
source .venv/bin/activate

# Install package and dependencies (includes imagetools)
uv pip install -e .

# Install external Java tools (bioformats2raw, raw2ometiff, bftools)
setup_prerequisites.sh
```

## Configuration

Copy and edit the configuration file:

```bash
cp config/deriva_imaging.json ~/.deriva_imaging.json
# Edit the file to set your hostname, paths, and credentials
```

See [config/README.md](config/README.md) for detailed configuration options.

## Running

### As a polling server

```bash
export DERIVA_IMAGING_POLL_SECONDS=300
export DERIVA_PIPELINE_HOSTNAME=iiif-dev.isi.edu
deriva-imaging-server --config ~/.deriva_imaging.json
```

### Process a single image by RID

```bash
deriva-imaging-client --config ~/.deriva_imaging.json --rid <RID>
```

## Running as a systemd service

Copy and edit the service file:

```bash
sudo cp service/deriva-imaging-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable deriva-imaging-pipeline
sudo systemctl start deriva-imaging-pipeline
```

See [service/README.md](service/README.md) for important notes when running the service
with SELinux enabled.

## License

Apache 2.0
