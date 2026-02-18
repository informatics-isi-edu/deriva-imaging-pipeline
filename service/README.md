# Setup

For SELinux environments, the executables referenced by the systemd service must 
satisfy the SELinux policy. This requires installing in a standard system location 
or assigning the necessary SELinux type (e.g., `bin_t`) to the executable's install 
location.

To install in a standard system location:

* Set `UV_PYTHON_INSTALL_DIR=/opt/uv-python` to instruct `uv` to install python under `/opt`
* Clone this repository under `/opt` and follow the usual installation instructions
* For good measure, you may want to install `uv` itself in `/usr/local/bin` by setting `UV_INSTALL_DIR`

See `uv` documentation for up-to-date instructions for the above. See SELinux documentation, 
for a complete explanation and alternatives to the above instructions.
