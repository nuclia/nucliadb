import tarfile
from pathlib import Path
from uuid import uuid4

from nucliadb.export_import import codecs
from nucliadb.export_import.context import ExporterContext


async def export_kb(context: ExporterContext, kbid: str, dest_dir: str) -> Path:
    # Create a file where all encoded resources are stored
    resources_file = Path(f"{dest_dir}/resources")
    with open(resources_file, "w") as resfile:
        # Create the export tar file
        export_tarfile = Path(f"{dest_dir}/{kbid}.export.tar.bz2")
        with tarfile.open(export_tarfile, mode="w:bz2") as tar:
            async for bm in context.data_manager.iter_broker_messages(kbid):
                # Write the encoded resource to the resources file
                resfile.write(codecs.encode_bm(bm))

                # Download each binary contained in the resource
                # and add it to the tar file
                binary_aux_file = f"{dest_dir}/{uuid4().hex}"
                for cf in context.data_manager.get_binaries(bm):
                    with open(binary_aux_file, "wb") as binary_file:
                        await context.data_manager.download_cf_to_file(cf, binary_file)
                    tar.add(binary_aux_file, cf.uri)

            # Write the encoded entities and labels to the resources file
            entities = await context.data_manager.get_entities(kbid)
            resfile.write(codecs.encode_entities(entities))

            labels = await context.data_manager.get_labels(kbid)
            resfile.write(codecs.encode_labels(labels))

            # Add the resources file to the tar file
            tar.add(resources_file, "resources")

    return export_tarfile
