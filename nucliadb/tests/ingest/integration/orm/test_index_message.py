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

import random
import time
import uuid
from datetime import datetime
from typing import Optional

import pytest
from faker import Faker

from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.index_message import IndexMessageBuilder
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import noderesources_pb2
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import utils_pb2 as upb
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.storages.storage import Storage
from tests.ingest.fixtures import TEST_CLOUDFILE, THUMBNAIL, add_field_id, create_resource, kb_vectorsets


async def test_generate_index_message_bw_compat(
    storage, maindb_driver: Driver, dummy_nidx_utility, knowledgebox_ingest: str
):
    full_resource = await create_resource(storage, maindb_driver, knowledgebox_ingest)

    async with maindb_driver.transaction(read_only=True) as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
        resource = await kb_obj.get(full_resource.uuid)
        assert resource is not None

        # Make sure that both the old and new way of generating index messages are equivalent
        imv1 = (await resource.generate_index_message(reindex=True)).brain
        imv2 = await IndexMessageBuilder(resource).full(reindex=True)

        # Relation fields to remove is not fully implemented in v1, so we skip it from the comparison
        imv1.ClearField("relation_fields_to_delete")
        imv2.ClearField("relation_fields_to_delete")

        # field_relations needs special treatment because it is a map of repeated fields
        assert flatten_field_relations(imv1) == flatten_field_relations(imv2)
        imv1.ClearField("field_relations")
        imv2.ClearField("field_relations")

        assert imv1 == imv2


def flatten_field_relations(r: noderesources_pb2.Resource):
    result = set()
    for field, field_relations in r.field_relations.items():
        for field_relation in field_relations.relations:
            to = field_relation.relation.to
            source = field_relation.relation.source
            result.add(
                (
                    field,
                    field_relation.relation.relation,
                    (
                        to.ntype,
                        to.subtype,
                        to.value,
                    ),
                    (
                        source.ntype,
                        source.subtype,
                        source.value,
                    ),
                )
            )


async def test_for_writer_bm_with_prefilter_update(
    storage, maindb_driver: Driver, dummy_nidx_utility, knowledgebox_ingest: str
):
    full_resource = await create_resource(storage, maindb_driver, knowledgebox_ingest)

    async with maindb_driver.transaction(read_only=True) as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
        resource = await kb_obj.get(full_resource.uuid)
        assert resource is not None
        writer_bm = BrokerMessage(
            kbid=knowledgebox_ingest,
            uuid=resource.uuid,
            reindex=True,
        )

        im = await IndexMessageBuilder(resource).for_writer_bm([writer_bm], resource_created=False)

        # title, summary, file, link, conv and text
        total_fields = 6

        # Only the texts and relations index is reindexed
        assert len(im.texts) == total_fields
        assert len(im.texts_to_delete) == total_fields
        assert len(im.field_relations) == 1  # a/metadata
        assert "a/metadata" in im.field_relations
        assert im.relation_fields_to_delete == ["a/metadata"]

        # Others are not
        assert len(im.paragraphs) == 0
        assert len(im.paragraphs_to_delete) == 0
        assert len(im.vector_prefixes_to_delete) == 0


@pytest.fixture(scope="function")
async def big_resource(storage, maindb_driver, knowledgebox_ingest):
    return await create_big_resource(storage, maindb_driver, knowledgebox_ingest)


async def test_big_resource(
    storage, maindb_driver: Driver, dummy_nidx_utility, knowledgebox_ingest: str, big_resource
):
    times_new = []
    times_old = []

    async with maindb_driver.transaction(read_only=True) as txn:
        for i in range(10):
            im_builder = IndexMessageBuilder(big_resource)
            start_new = time.perf_counter()
            im = await im_builder.full(reindex=True)
            end_new = time.perf_counter()
            times_new.append(end_new - start_new)

            # start_old = time.perf_counter()
            # await big_resource.generate_index_message(reindex=True)
            # end_old = time.perf_counter()
            # times_old.append(end_old - start_old)

    print("=============================")
    print("Times for new way")
    print("=============================")
    print(f"p50: {int(1000 * (sum(times_new) / len(times_new)))} ms")
    print(f"p90: {int(1000 * (sorted(times_new)[int(len(times_new) * 0.9)]))} ms")
    print("==============================")
    # print("Times for old way")
    # print("==============================")
    # print(f"p50: {int(1000 * (sum(times_old) / len(times_old)))} ms")
    # print(f"p90: {int(1000 * (sorted(times_old)[int(len(times_old) * 0.9)]))} ms")
    # print("==============================")


async def create_big_resource(storage: Storage, driver: Driver, knowledgebox_ingest: str) -> Resource:
    fake = Faker()
    n_basic_metadata_entries = 2000
    total_classifications = 2000
    n_basic_user_classifications = 1000
    n_fields_per_type = 10
    n_user_field_metadata = 1000
    n_paragraphs_per_field = 1000
    paragraph_length = 20
    n_user_relations = 3000
    n_origin_tags = 100
    n_origin_metadata_entries = 1000
    extracted_text_max_characters = 1000

    classifications = [
        rpb.Classification(
            labelset="ADDRESS",
            label=fake.address(),
        )
        for _ in range(total_classifications)
    ]

    field_types = {
        rpb.FieldType.TEXT: "t",
        rpb.FieldType.FILE: "f",
        rpb.FieldType.LINK: "u",
        rpb.FieldType.CONVERSATION: "c",
    }
    async with driver.transaction() as txn:
        rid = str(uuid.uuid4())
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
        test_resource = await kb_obj.add_resource(uuid=rid, slug="slug")
        await test_resource.set_slug()

        # 1.  ROOT ELEMENTS
        # 1.1 BASIC
        basic = rpb.Basic(
            title=fake.sentence(nb_words=30),
            summary=fake.sentence(nb_words=300),
            icon="text/plain",
            thumbnail="/file",
            last_seqid=1,
            last_account_seq=2,
        )
        for i in range(n_basic_metadata_entries):
            basic.metadata.metadata[fake.word()] = fake.word()
        basic.metadata.language = "ca"
        basic.metadata.useful = True
        basic.metadata.status = rpb.Metadata.Status.PROCESSED
        for _ in range(n_basic_user_classifications):
            classif = random.choice(classifications)
            if classif not in basic.usermetadata.classifications:
                basic.usermetadata.classifications.append(classif)
        for _ in range(n_user_field_metadata):
            field_type = random.choice(list(field_types.keys()))
            field_key = random.randint(0, n_fields_per_type)
            field_type_str = field_types[field_type]
            ufm = rpb.UserFieldMetadata(
                field=rpb.FieldID(field_type=field_type, field=str(field_key)),
            )
            for i in range(10):
                classification = random.choice(classifications)
                if classification not in ufm.paragraphs:
                    paragraph_index = random.randint(0, n_paragraphs_per_field)
                    start = paragraph_index * paragraph_length
                    end = start + paragraph_length
                    ufm.paragraphs.append(
                        rpb.ParagraphAnnotation(
                            classifications=[classification],
                            key=f"{rid}/{field_type_str}/{field_key}/{start}-{end}",
                        )
                    )
            basic.fieldmetadata.append(ufm)
        basic.created.FromDatetime(datetime.now())
        basic.modified.FromDatetime(datetime.now())
        await test_resource.set_basic(basic)

        # 1.2 USER RELATIONS
        rels = rpb.Relations()
        base_relation = upb.Relation(
            relation=upb.Relation.CHILD,
            source=upb.RelationNode(value=rid, ntype=upb.RelationNode.NodeType.RESOURCE),
        )
        for i in range(n_user_relations):
            r = upb.Relation()
            r.CopyFrom(base_relation)
            r.to.CopyFrom(upb.RelationNode(
                value=fake.word(),
                ntype=upb.RelationNode.NodeType.RESOURCE,
            ))
            rels.relations.append(r)
        await test_resource.set_user_relations(rels)

        # 1.3 ORIGIN
        o2 = rpb.Origin()
        o2.source = rpb.Origin.Source.API
        o2.source_id = "My Source"
        o2.created.FromDatetime(datetime.now())
        o2.modified.FromDatetime(datetime.now())
        o2.tags.extend([f"tag{i}" for i in range(n_origin_tags)])
        o2.metadata.update(**{fake.word(): fake.word() for _ in range(n_origin_metadata_entries)})
        await test_resource.set_origin(o2)

        # 2.  FIELDS
        #
        # Add an example of each of the files, containing all possible metadata
        # Title
        title_field = await test_resource.get_field("title", rpb.FieldType.GENERIC, load=False)
        await make_field(
            title_field,
            rpb.FieldID(field_type=rpb.FieldType.GENERIC, field="title"),
            basic.title,
        )

        # Summary
        summary_field = await test_resource.get_field("summary", rpb.FieldType.GENERIC, load=False)
        await make_field(
            summary_field,
            rpb.FieldID(field_type=rpb.FieldType.GENERIC, field="summary"),
            basic.summary,
        )

        # 2.1 FILE FIELDS
        for i in range(n_fields_per_type):
            ff = rpb.FieldFile(
                language="es",
            )
            ff.added.FromDatetime(datetime.now())
            ff.file.CopyFrom(TEST_CLOUDFILE)
            file_field = await test_resource.set_field(rpb.FieldType.FILE, str(i), ff)
            await add_field_id(test_resource, file_field)
            await make_field(
                file_field,
                rpb.FieldID(field_type=rpb.FieldType.FILE, field=str(i)),
                extracted_text=fake.text(max_nb_chars=extracted_text_max_characters),
                paragraph_size=paragraph_length,
                classifications=classifications,
            )

        # 2.2 LINK FIELDS
        for i in range(n_fields_per_type):
            li = rpb.FieldLink(
                uri="htts://nuclia.cloud",
                language="ca",
            )
            li.added.FromDatetime(datetime.now())
            li.headers["AUTHORIZATION"] = "Bearer xxxxx"
            linkfield: Field = await test_resource.set_field(rpb.FieldType.LINK, str(i), li)
            ex1 = rpb.LinkExtractedData()
            ex1.date.FromDatetime(datetime.now())
            ex1.language = "ca"
            ex1.title = "My Title"
            ex1.field = "link1"
            ex1.link_preview.CopyFrom(THUMBNAIL)
            ex1.link_thumbnail.CopyFrom(THUMBNAIL)
            await linkfield.set_link_extracted_data(ex1)
            await add_field_id(test_resource, linkfield)
            await make_field(
                linkfield,
                rpb.FieldID(field_type=rpb.FieldType.LINK, field=str(i)),
                extracted_text=fake.text(max_nb_chars=extracted_text_max_characters),
                paragraph_size=paragraph_length,
                classifications=classifications,
            )

        # 2.3 TEXT FIELDS
        for i in range(n_fields_per_type):
            t23 = rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
            textfield = await test_resource.set_field(rpb.FieldType.TEXT, str(i), t23)
            await add_field_id(test_resource, textfield)
            await make_field(
                textfield,
                rpb.FieldID(field_type=rpb.FieldType.TEXT, field=str(i)),
                extracted_text=fake.text(max_nb_chars=extracted_text_max_characters),
                paragraph_size=paragraph_length,
                classifications=classifications,
            )


        # 2.4 CONVERSATION FIELD
        def make_message(text: str, files: Optional[list[rpb.CloudFile]] = None) -> rpb.Message:
            msg = rpb.Message(
                who="myself",
            )
            msg.timestamp.FromDatetime(datetime.now())
            msg.content.text = text
            msg.content.format = rpb.MessageContent.Format.PLAIN

            if files:
                for file in files:
                    msg.content.attachments.append(file)
            return msg

        for i in range(n_fields_per_type):
            c2 = rpb.Conversation()
            for i in range(300):
                new_message = make_message(f"{i} hello")
                if i == 33:
                    new_message = make_message(f"{i} hello", files=[TEST_CLOUDFILE, THUMBNAIL])
                c2.messages.append(new_message)
            convfield = await test_resource.set_field(rpb.FieldType.CONVERSATION, str(i), c2)
            await add_field_id(test_resource, convfield)
            await make_field(
                convfield,
                rpb.FieldID(field_type=rpb.FieldType.CONVERSATION, field=str(i)),
                extracted_text=fake.text(max_nb_chars=extracted_text_max_characters),
                paragraph_size=paragraph_length,
                classifications=classifications,
            )
        await txn.commit()
        return test_resource



async def make_field(
    field: Field,
    fieldid: rpb.FieldID,
    extracted_text: str,
    paragraph_size: int = 20,
    classifications: list[rpb.Classification] = [],
):
    vectorsets = await kb_vectorsets(field.resource.kb)

    await field.set_extracted_text(
        make_extracted_text(fieldid, body=extracted_text)
    )
    await field.set_field_metadata(
        make_field_metadata(
            field.resource.uuid,
            fieldid,
            extracted_text=extracted_text,
            paragraph_size=paragraph_size,
            classifications=classifications,
            n_processor_entities=1000,
            n_data_augmentation_entities=1000,
        )
    )
    await field.set_large_field_metadata(make_field_large_metadata(field.id))
    assert len(vectorsets) > 0, "KBs must have at least a vectorset"
    for idx, vs in enumerate(vectorsets):
        await field.set_vectors(
            make_extracted_vectors(
                fieldid, vs, idx,
                extracted_text=extracted_text,
                paragraph_size=paragraph_size,
            ),
            vectorset=vs.vectorset_id,
            storage_key_kind=vs.storage_key_kind,
        )


def make_extracted_text(field_id: rpb.FieldID, body: str) -> rpb.ExtractedTextWrapper:
    ex1 = rpb.ExtractedTextWrapper()
    ex1.field.CopyFrom(field_id)
    ex1.body.text = body
    return ex1


def make_field_metadata(
    rid: str,
    field: rpb.FieldID,
    extracted_text: str,
    paragraph_size: int = 20,
    classifications: list[rpb.Classification] = [],
    n_processor_entities: int = 1000,
    n_data_augmentation_entities: int = 1000,
) -> rpb.FieldComputedMetadataWrapper:

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.CopyFrom(field)
    fcm.metadata.metadata.links.append("https://nuclia.com")

    # Paragraphs computed
    for i in range(0, len(extracted_text), paragraph_size):
        start = i
        end = min(i + paragraph_size, len(extracted_text))
        sentence = rpb.Sentence(
            start=start,
            end=end,
            key=f"{rid}/{FIELD_TYPE_PB_TO_STR[field.field_type]}/{field.field}/0/{start}-{end}",
        )
        paragraph = rpb.Paragraph(start=start, end=end, sentences=[sentence])
        if classifications:
            pclassif = random.choice(classifications)
            paragraph.classifications.append(pclassif)
        fcm.metadata.metadata.paragraphs.append(paragraph)

    # Field classifications
    for _ in range(random.randint(10, 20)):
        if classifications:
            cl = random.choice(classifications)
            if cl not in fcm.metadata.metadata.classifications:
                fcm.metadata.metadata.classifications.append(cl)

    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_summary.FromDatetime(datetime.now())
    fcm.metadata.metadata.thumbnail.CopyFrom(THUMBNAIL)

    # Processor entities
    processor_entities = fcm.metadata.metadata.entities["processor"].entities
    for i in range(n_processor_entities):
        label = f"ENTITY-{i}"
        text = f"document-{i}"
        positions = []
        for i in range(random.randint(2, 6)):
            start = random.randint(0, len(extracted_text) - 1)
            end = random.randint(start + 1, len(extracted_text))
            positions.append(rpb.Position(start=start, end=end))
        entity = rpb.FieldEntity(
            text=text,
            label=label,
            positions=positions,
        )
        processor_entities.append(entity)

    # Data Augmentation entities
    data_augmentation_entities = fcm.metadata.metadata.entities["my-task-id"].entities
    for i in range(n_data_augmentation_entities):
        label = f"NOUN-{i}"
        text = f"document-{i}"
        positions = []
        for i in range(random.randint(2, 6)):
            start = random.randint(0, len(extracted_text) - 1)
            end = random.randint(start + 1, len(extracted_text))
            positions.append(rpb.Position(start=start, end=end))
        entity = rpb.FieldEntity(
            text=text,
            label=label,
            positions=positions,
        )
        data_augmentation_entities.append(entity)
    fcm.metadata.metadata.mime_type = "text/html"
    return fcm




def make_field_large_metadata(field_id: str) -> rpb.LargeComputedMetadataWrapper:
    ex1 = rpb.LargeComputedMetadataWrapper()
    ex1.field.CopyFrom(rpb.FieldID(field_type=rpb.FieldType.TEXT, field=field_id))
    en1 = rpb.Entity(token="tok1", root="tok", type="NAME")
    en2 = rpb.Entity(token="tok2", root="tok2", type="NAME")
    ex1.real.metadata.entities.append(en1)
    ex1.real.metadata.entities.append(en2)
    ex1.real.metadata.tokens["tok"] = 3
    return ex1


def make_extracted_vectors(
    field: rpb.FieldID,
    vectorset: VectorSetConfig,
    vectorset_idx: int,
    extracted_text: str,
    paragraph_size: int = 20,
) -> rpb.ExtractedVectorsWrapper:
    ex1 = rpb.ExtractedVectorsWrapper()
    ex1.field.CopyFrom(field)
    ex1.vectorset_id = vectorset.vectorset_id
    dimension = vectorset.vectorset_index_config.vector_dimension
    for i in range(0, len(extracted_text), paragraph_size):
        start = i
        end = min(i + paragraph_size, len(extracted_text))
        vector = rpb.Vector(
            start=start,
            end=end,
            vector=[float(vectorset_idx)] * dimension,
        )
        ex1.vectors.vectors.vectors.append(vector)
    return ex1
