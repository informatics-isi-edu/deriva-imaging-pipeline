# deriva-imaging-pipeline
Tools and configuration for the DERIVA imaging pipeline

# Installation

```
git clone https://github.com/informatics-isi-edu/deriva-imaging-pipeline.git
cd deriva-imaging-pipeline/pipeline/deriva-imaging-pipeline
pip3 install --upgrade --no-deps .
```

# Running

```
env DERIVA_IMAGING_POLL_SECONDS=300 DERIVA_PIPELINE_HOSTNAME=iiif-dev.isi.edu deriva-imaging-server --config deriva-imaging-pipeline/config/deriva_imaging.json 
```
or to run it just for one `RID`:

```
deriva-imaging-client --config deriva-imaging-pipeline/config/deriva_imaging.json --rid <RID>
```

