# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import logging

from nidx_protos import noderesources_pb2, nodewriter_pb2
from nidx_protos.noderesources_pb2 import Resource as PBBrainResource

from nucliadb.common import datamanagers
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR
from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.orm.exceptions import ResourceNotIndexable
from nucliadb_models import content_types
from nucliadb_models.common import CloudLink
from nucliadb_models.content_types import GENERIC_MIME_TYPE
from nucliadb_protos import (
    resources_pb2,
    writer_pb2,
)
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import FieldID
from nucliadb_telemetry import errors

logger = logging.getLogger("ingest-processor")

MESSAGE_TO_NOTIFICATION_SOURCE = {
    writer_pb2.BrokerMessage.MessageSource.WRITER: writer_pb2.NotificationSource.WRITER,
    writer_pb2.BrokerMessage.MessageSource.PROCESSOR: writer_pb2.NotificationSource.PROCESSOR,
}


def validate_indexable_resource(resource: noderesources_pb2.Resource) -> None:
    """
    It would be more optimal to move this to another layer but it'd also make the code
    more difficult to grok and test because we'd need to move processable check and throw
    an exception in the middle of a bunch of processing logic.

    As it is implemented right now, we just do the check if a resource is indexable right
    before we actually try to index it and not buried it somewhere else in the code base.

    This is still an edge case.
    """
    num_paragraphs = 0
    first_exceeded = ""
    for field_id, fparagraph in resource.paragraphs.items():
        # this count should not be very expensive to do since we don't have
        # a lot of different fields and we just do a count on a dict
        num_paragraphs += len(fparagraph.paragraphs)
        if num_paragraphs > cluster_settings.max_resource_paragraphs:
            first_exceeded = field_id

    if num_paragraphs > cluster_settings.max_resource_paragraphs:
        raise ResourceNotIndexable(
            first_exceeded,
            f"Resource has too many paragraphs ({num_paragraphs}) and cannot be indexed. "
            f"The maximum number of paragraphs per resource is {cluster_settings.max_resource_paragraphs}",
        )


def trim_entity_facets(index_message: PBBrainResource) -> list[tuple[str, str]]:
    max_entities = cluster_settings.max_entity_facets
    warnings = []
    for field_id, text_info in index_message.texts.items():
        if len(text_info.labels) > max_entities:
            new_labels = []
            entity_count = 0
            truncated = False
            for label in text_info.labels:
                if label.startswith("/e/"):
                    if entity_count < max_entities:
                        new_labels.append(label)
                    else:
                        truncated = True
                    entity_count += 1
                else:
                    new_labels.append(label)
            if truncated:
                warnings.append(
                    (
                        field_id,
                        f"Too many detected entities. Only the first {max_entities} will be available as facets for filtering",
                    )
                )
                text_info.ClearField("labels")
                text_info.labels.extend(new_labels)

    return warnings


def to_index_message_source(message: writer_pb2.BrokerMessage):
    if message.source == writer_pb2.BrokerMessage.MessageSource.WRITER:
        return nodewriter_pb2.IndexMessageSource.WRITER
    elif message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
        return nodewriter_pb2.IndexMessageSource.PROCESSOR
    else:  # pragma: no cover
        msg = f"Processor received a broker message with unexpected source! {message.source}"
        logger.error(msg)
        errors.capture_exception(Exception(msg))
        return nodewriter_pb2.IndexMessageSource.PROCESSOR


def has_vectors_operation(index_message: PBBrainResource) -> bool:
    """
    Returns True if the index message has any vectors to index or to delete.
    """
    if any([len(deletions.items) for deletions in index_message.vector_prefixes_to_delete.values()]):
        return True
    for field_paragraphs in index_message.paragraphs.values():
        for paragraph in field_paragraphs.paragraphs.values():
            if len(paragraph.sentences) > 0:
                return True
            for vectorset_sentences in paragraph.vectorsets_sentences.values():
                if len(vectorset_sentences.sentences) > 0:
                    return True
    return False


# Immutable basic fields that are already set should not be overwritten by incoming payloads.
_BASIC_IMMUTABLE_FIELDS = ("icon",)


def _merge_basic(
    current: PBBasic,
    payload: PBBasic,
    deleted_fields: list[FieldID],
) -> PBBasic:
    """
    Merge an incoming basic payload onto the existing basic, applying all
    deduplication and immutability rules, then return the result ready to persist.
    """
    assert current.ByteSize(), "Expected an existing resource with non-empty basic"
    merged = PBBasic()
    merged.CopyFrom(current)

    if merged != payload:
        for field in _BASIC_IMMUTABLE_FIELDS:
            # Immutable basic fields that are already set are cleared from
            # the payload so that they are not overwritten.
            if getattr(merged, field, "") != "":
                payload.ClearField(field)  # type: ignore[arg-type]

        merged.MergeFrom(payload)

        # Prevent duplicated languages
        unique_languages = set(merged.metadata.languages)
        merged.metadata.ClearField("languages")
        merged.metadata.languages.extend(unique_languages)

        # Prevent duplicated labels
        unique_labels = set(merged.labels)
        merged.ClearField("labels")
        merged.labels.extend(unique_labels)

        # Update processing status: only overwrite when the payload signals
        # that the resource is useful (i.e. coming from a real processing run).
        if payload.HasField("metadata") and payload.metadata.useful:
            merged.metadata.status = payload.metadata.status

        # We force the usermetadata classifications to be exactly what the
        # payload defines (no merging).
        if payload.HasField("usermetadata"):
            merged.usermetadata.CopyFrom(payload.usermetadata)

        if len(payload.fieldmetadata):
            # Keep only the last fieldmetadata item for each field. API
            # users are responsible to manage fieldmetadata properly.
            fields: list[str] = []
            positions: dict[str, int] = {}
            for i, fieldmetadata in enumerate(merged.fieldmetadata):
                field_id = (
                    f"{FIELD_TYPE_PB_TO_STR[fieldmetadata.field.field_type]}/{fieldmetadata.field.field}"
                )
                if field_id not in fields:
                    fields.append(field_id)
                positions[field_id] = i

            updated = [merged.fieldmetadata[positions[f]] for f in fields]
            del merged.fieldmetadata[:]
            merged.fieldmetadata.extend(updated)

    # Some basic fields are computed off field metadata.
    # Recompute upon field deletions.
    if deleted_fields:
        delete_basic_computedmetadata_classifications(merged, deleted_fields=deleted_fields)

    return merged


def _should_reset_title(basic: PBBasic, resource_uuid: str) -> bool:
    """
    When a user creates a resource without a title, the system will try to set the title from
    some extracted metadata like the title of a link field or the extracted title of a file field.
    """
    return basic.title in ("", resource_uuid) or basic.reset_title


def _should_set_title_from_files(basic: PBBasic, resource_uuid: str, filenames: list[str]) -> bool:
    return _should_reset_title(basic, resource_uuid) or basic.title in filenames


def _should_set_title_from_link(basic: PBBasic, resource_uuid: str, extracted_title: str) -> bool:
    return extracted_title != "" and (
        _should_reset_title(basic, resource_uuid) or basic.title.startswith("http")
    )


def _apply_extracted_basic_updates(
    message: writer_pb2.BrokerMessage,
    basic: PBBasic,
    resource_uuid: str,
    filenames: list[str],
) -> None:
    """
    Inspect the extracted-data sections of a broker message and apply all
    side-effect mutations to basic (icon, thumbnail, summary, title, languages,
    computed classifications). Does not persist — caller is responsible for that.
    """
    # Icon from text field format
    maybe_update_basic_icon(basic, get_text_field_mimetype(message))

    extracted_languages: list[str] = []

    # Link extracted data
    for led in message.link_extracted_data:
        maybe_update_basic_thumbnail(basic, led.link_thumbnail, message.kbid)
        maybe_update_basic_icon(basic, "application/stf-link")
        maybe_update_basic_summary(basic, led.description)
        extracted_languages.append(led.language)
        # Update title from the first link that has one and current title looks auto-generated
        if _should_set_title_from_link(basic, resource_uuid, led.title):
            # FIXME: this doesn't properly index the new title. See sc-6088 for more details
            basic.title = led.title
            basic.reset_title = False

    # File extracted data
    for fed in message.file_extracted_data:
        maybe_update_basic_icon(basic, fed.icon)
        maybe_update_basic_thumbnail(basic, fed.file_thumbnail, message.kbid)
        extracted_languages.append(fed.language)

    # Update title from the first file that has one (if title looks auto-generated)
    if _should_set_title_from_files(basic, resource_uuid, filenames):
        for fed in message.file_extracted_data:
            if fed.title:
                basic.title = fed.title
                basic.reset_title = False
                break

    # Field computed metadata
    for fcmw in message.field_metadata:
        maybe_update_basic_summary(basic, fcmw.metadata.metadata.summary)
        maybe_update_basic_thumbnail(basic, fcmw.metadata.metadata.thumbnail, message.kbid)
        update_basic_computedmetadata_classifications(basic, fcmw)
        extracted_languages.extend(extract_field_metadata_languages(fcmw))

    update_basic_languages(basic, extracted_languages)


async def compute_resource_status(
    txn: Transaction,
    kbid: str,
    uuid: str,
    basic: PBBasic,
) -> None:
    """
    Compute and set the resource-level processing status on basic by inspecting
    the status of all individual fields.
    """
    field_ids = await datamanagers.fields.get_all_field_ids(txn, kbid=kbid, rid=uuid)
    if field_ids is None:
        # No fields, it is processed
        basic.metadata.status = resources_pb2.Metadata.Status.PROCESSED
        return

    processed_fields = [f for f in field_ids.fields if is_processed_field(f)]
    if not processed_fields:
        # No processed fields, it is pending
        basic.metadata.status = resources_pb2.Metadata.Status.PROCESSED
        return

    field_statuses = await datamanagers.fields.get_statuses(
        txn, kbid=kbid, rid=uuid, fields=processed_fields
    )

    # If any field is processing -> PENDING
    if any(f.status == writer_pb2.FieldStatus.Status.PENDING for f in field_statuses):
        basic.metadata.status = resources_pb2.Metadata.Status.PENDING
    # If we have any non-DA error -> ERROR
    elif any(
        f.status == writer_pb2.FieldStatus.Status.ERROR
        and any(
            e.source_error.severity == writer_pb2.Error.Severity.ERROR
            and e.source_error.code != writer_pb2.Error.ErrorCode.DATAAUGMENTATION
            for e in f.errors
        )
        for f in field_statuses
    ):
        basic.metadata.status = resources_pb2.Metadata.Status.ERROR
    # Otherwise (everything processed or we only have DA errors) -> PROCESSED
    else:
        basic.metadata.status = resources_pb2.Metadata.Status.PROCESSED


def is_processed_field(field: resources_pb2.FieldID) -> bool:
    """
    Title and summary fields are not considered processed fields
    """
    return field not in (
        resources_pb2.FieldID(field="title", field_type=resources_pb2.FieldType.GENERIC),
        resources_pb2.FieldID(field="summary", field_type=resources_pb2.FieldType.GENERIC),
    )


def delete_basic_computedmetadata_classifications(basic: PBBasic, deleted_fields: list[FieldID]) -> bool:
    """
    We keep a copy of field classifications computed by the processing engine at the basic object
    so that users can easily access them without having to load the field metadata from the storage.

    This funcion removes the field classifications for the fields that have been deleted.
    Returns whether the basic was modified.
    """
    if len(deleted_fields) == 0:
        # Nothing to delete
        return False
    new_field_classifications = [
        fc for fc in basic.computedmetadata.field_classifications if fc.field not in deleted_fields
    ]
    if len(new_field_classifications) == len(basic.computedmetadata.field_classifications):
        # No changes
        return False

    basic.computedmetadata.ClearField("field_classifications")
    basic.computedmetadata.field_classifications.extend(new_field_classifications)
    return True


def maybe_update_basic_summary(basic: PBBasic, summary_text: str) -> bool:
    if basic.summary or not summary_text:
        return False
    basic.summary = summary_text
    return True


def maybe_update_basic_icon(basic: PBBasic, mimetype: str | None) -> bool:
    if basic.icon not in (None, "", "application/octet-stream", GENERIC_MIME_TYPE):
        # Icon already set or detected
        return False

    if not mimetype:
        return False

    if not content_types.valid(mimetype):
        logger.warning(
            "Invalid mimetype. Skipping icon update.",
            extra={"mimetype": mimetype, "rid": basic.uuid, "slug": basic.slug},
        )
        return False

    basic.icon = mimetype
    return True


def maybe_update_basic_thumbnail(
    basic: PBBasic, thumbnail: resources_pb2.CloudFile | None, kbid: str
) -> bool:
    if basic.thumbnail or thumbnail is None:
        return False
    basic.thumbnail = CloudLink.format_reader_download_uri(thumbnail.uri)
    fix_kbid_in_thumbnail(basic, kbid)
    return True


def extract_field_metadata_languages(
    field_metadata: resources_pb2.FieldComputedMetadataWrapper,
) -> list[str]:
    languages: set[str] = set()
    languages.add(field_metadata.metadata.metadata.language)
    for _, splitted_metadata in field_metadata.metadata.split_metadata.items():
        languages.add(splitted_metadata.language)
    return list(languages)


PB_TEXT_FORMAT_TO_MIMETYPE = {
    resources_pb2.FieldText.Format.PLAIN: "text/plain",
    resources_pb2.FieldText.Format.HTML: "text/html",
    resources_pb2.FieldText.Format.RST: "text/x-rst",
    resources_pb2.FieldText.Format.MARKDOWN: "text/markdown",
    resources_pb2.FieldText.Format.JSON: "application/json",
    resources_pb2.FieldText.Format.KEEP_MARKDOWN: "text/markdown",
    resources_pb2.FieldText.Format.JSONL: "application/x-ndjson",
    resources_pb2.FieldText.Format.PLAIN_BLANKLINE_SPLIT: "text/plain+blankline",
}


def get_text_field_mimetype(bm: writer_pb2.BrokerMessage) -> str | None:
    if len(bm.texts) == 0:
        return None
    text_format = next(iter(bm.texts.values())).format
    return PB_TEXT_FORMAT_TO_MIMETYPE[text_format]


def update_basic_languages(basic: writer_pb2.Basic, languages: list[str]) -> bool:
    if len(languages) == 0:
        return False

    updated = False
    for language in languages:
        if not language:
            continue

        if basic.metadata.language == "":
            basic.metadata.language = language
            updated = True

        if language not in basic.metadata.languages:
            basic.metadata.languages.append(language)
            updated = True

    return updated


def update_basic_computedmetadata_classifications(
    basic: PBBasic, fcmw: resources_pb2.FieldComputedMetadataWrapper
) -> bool:
    """
    We keep a copy of field classifications computed by the processing engine at the basic object
    so that users can easily access them without having to load the field metadata from the storage.

    This function updates the basic object with the new field computed metadata.
    Returns whether the basic was modified.
    """
    some_deleted = delete_basic_computedmetadata_classifications(basic, [fcmw.field])

    fcfs = resources_pb2.FieldClassifications()
    fcfs.field.CopyFrom(fcmw.field)

    some_added = False
    if len(fcmw.metadata.metadata.classifications) > 0:
        some_added = True
        fcfs.classifications.extend(fcmw.metadata.metadata.classifications)

    for split_id, split in fcmw.metadata.split_metadata.items():
        if split_id not in fcmw.metadata.deleted_splits:
            if len(split.classifications) > 0:
                some_added = True
                fcfs.classifications.extend(split.classifications)
    if some_added:
        basic.computedmetadata.field_classifications.append(fcfs)
    return some_added or some_deleted


def fix_kbid_in_thumbnail(basic: PBBasic, kbid: str):
    if basic.thumbnail.startswith("/kb/") and not basic.thumbnail.startswith(f"/kb/{kbid}/"):
        # Replace the kbid in the thumbnail if it doesn't match the current kbid. This is necessary for
        # resources that have been backed up and we are restoring them to a different kbid.
        parts = basic.thumbnail.split("/", 3)
        parts[2] = kbid
        basic.thumbnail = "/".join(parts)
