from async_processor.sidecar.sidecar import app

# TODO: remove app from here.
# If someone just want to import async_processor.sidecar.types, Settings will get triggered.
# Before we remove this we need to update the command that we use for sidecar.
__all__ = ["app"]
