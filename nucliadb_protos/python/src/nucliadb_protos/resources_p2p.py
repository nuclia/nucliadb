# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.0.3](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.5 
# Pydantic Version: 2.10.4 
from .utils_p2p import ExtractedText
from .utils_p2p import Relation
from .utils_p2p import UserVectorSet
from .utils_p2p import UserVectorsList
from .utils_p2p import VectorObject
from datetime import datetime
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
import typing

class FieldType(IntEnum):
    FILE = 0
    LINK = 1
    TEXT = 4
    GENERIC = 6
    CONVERSATION = 7

class CloudFile(BaseModel):
    class Source(IntEnum):
        FLAPS = 0
        GCS = 1
        S3 = 2
        LOCAL = 3
        EXTERNAL = 4
        EMPTY = 5
        EXPORT = 6
        POSTGRES = 7
        AZURE = 8

    uri: str = Field(default="")
    size: int = Field(default=0)
    content_type: str = Field(default="")
    bucket_name: str = Field(default="")
    source: "CloudFile.Source" = Field(default=0)
    filename: str = Field(default="")
# Temporal upload information
    resumable_uri: str = Field(default="")
    offset: int = Field(default=0)
    upload_uri: str = Field(default="")
    parts: typing.List[str] = Field(default_factory=list)
    old_uri: str = Field(default="")
    old_bucket: str = Field(default="")
    md5: str = Field(default="")

class Metadata(BaseModel):
    class Status(IntEnum):
        PENDING = 0
        PROCESSED = 1
        ERROR = 2
        BLOCKED = 3
        EXPIRED = 4

    metadata: typing.Dict[str, str] = Field(default_factory=dict)
    language: str = Field(default="")
    languages: typing.List[str] = Field(default_factory=list)
    useful: bool = Field(default=False)
    status: Status = Field(default=0)

class Classification(BaseModel):
    labelset: str = Field(default="")
    label: str = Field(default="")
    cancelled_by_user: bool = Field(default=False)
    split: str = Field(default="")# On field classification we need to set on which split is the classification

class UserMetadata(BaseModel):
    classifications: typing.List[Classification] = Field(default_factory=list)
    relations: typing.List[Relation] = Field(default_factory=list)

class TokenSplit(BaseModel):
    token: str = Field(default="")
    klass: str = Field(default="")
    start: int = Field(default=0)
    end: int = Field(default=0)
    cancelled_by_user: bool = Field(default=False)
    split: str = Field(default="")

class ParagraphAnnotation(BaseModel):
    key: str = Field(default="")
    classifications: typing.List[Classification] = Field(default_factory=list)

class VisualSelection(BaseModel):
    label: str = Field(default="")
    top: float = Field(default=0.0)
    left: float = Field(default=0.0)
    right: float = Field(default=0.0)
    bottom: float = Field(default=0.0)
# Token IDs are the indexes in PageStructure
    token_ids: typing.List[int] = Field(default_factory=list)

class PageSelections(BaseModel):
    page: int = Field(default=0)
    visual: typing.List[VisualSelection] = Field(default_factory=list)

class Question(BaseModel):
    text: str = Field(default="")
    language: str = Field(default="")
    ids_paragraphs: typing.List[str] = Field(default_factory=list)

class Answers(BaseModel):
    text: str = Field(default="")
    language: str = Field(default="")
    ids_paragraphs: typing.List[str] = Field(default_factory=list)
    reason: str = Field(default="")

class QuestionAnswer(BaseModel):
    question: Question = Field()
    answers: typing.List[Answers] = Field(default_factory=list)

class QuestionAnswerAnnotation(BaseModel):
    question_answer: QuestionAnswer = Field()
    cancelled_by_user: bool = Field(default=False)

class FieldID(BaseModel):
    field_type: FieldType = Field(default=0)
    field: str = Field(default="")

class UserFieldMetadata(BaseModel):
    token: typing.List[TokenSplit] = Field(default_factory=list)
    paragraphs: typing.List[ParagraphAnnotation] = Field(default_factory=list)
    page_selections: typing.List[PageSelections] = Field(default_factory=list)
    question_answers: typing.List[QuestionAnswerAnnotation] = Field(default_factory=list)
    field: FieldID = Field()

class FieldClassifications(BaseModel):
    field: FieldID = Field()
    classifications: typing.List[Classification] = Field(default_factory=list)

class ComputedMetadata(BaseModel):
    field_classifications: typing.List[FieldClassifications] = Field(default_factory=list)

class Basic(BaseModel):
    class QueueType(IntEnum):
        PRIVATE = 0
        SHARED = 1

    slug: str = Field(default="")
    icon: str = Field(default="")
    title: str = Field(default="")
    summary: str = Field(default="")
    thumbnail: str = Field(default="")# reference to inner thumbnail
    layout: str = Field(default="")
    created: datetime = Field(default_factory=datetime.now)
    modified: datetime = Field(default_factory=datetime.now)
    metadata: Metadata = Field()
# Not Basic
    usermetadata: UserMetadata = Field()
    fieldmetadata: typing.List[UserFieldMetadata] = Field(default_factory=list)
    computedmetadata: ComputedMetadata = Field()
# Only for read operations
    uuid: str = Field(default="")
    labels: typing.List[str] = Field(default_factory=list)
# last processing seqid of the resource
    last_seqid: int = Field(default=0)
# last processing sequid (non nats) of this resource in the account queue
    last_account_seq: int = Field(default=0)
    queue: "Basic.QueueType" = Field(default=0)
    hidden: typing.Optional[bool] = Field(default=False)

class Origin(BaseModel):
    """
     Block behaviors
    """
    class Source(IntEnum):
        WEB = 0
        DESKTOP = 1
        API = 2

    source: "Origin.Source" = Field(default=0)
    source_id: str = Field(default="")
    url: str = Field(default="")
    created: datetime = Field(default_factory=datetime.now)
    modified: datetime = Field(default_factory=datetime.now)
    metadata: typing.Dict[str, str] = Field(default_factory=dict)
    tags: typing.List[str] = Field(default_factory=list)
    colaborators: typing.List[str] = Field(default_factory=list)
    filename: str = Field(default="")
    related: typing.List[str] = Field(default_factory=list)
    path: str = Field(default="")

class Extra(BaseModel):
    metadata: typing.Dict[str, typing.Any] = Field(default_factory=dict)

class Relations(BaseModel):
    relations: typing.List[Relation] = Field(default_factory=list)

class FieldRef(BaseModel):
    field_type: FieldType = Field(default=0)
    field_id: str = Field(default="")
    split: str = Field(default="")

class MessageContent(BaseModel):
    class Format(IntEnum):
        PLAIN = 0
        HTML = 1
        MARKDOWN = 2
        RST = 3
        KEEP_MARKDOWN = 4

    text: str = Field(default="")
    format: "MessageContent.Format" = Field(default=0)
    attachments: typing.List[CloudFile] = Field(default_factory=list)
# Store links or fields on root resource fields to be processed as normal fields
    attachments_fields: typing.List[FieldRef] = Field(default_factory=list)

class Message(BaseModel):
    class MessageType(IntEnum):
        UNSET = 0
        QUESTION = 1
        ANSWER = 2

    timestamp: datetime = Field(default_factory=datetime.now)
    who: str = Field(default="")
    to: typing.List[str] = Field(default_factory=list)
    content: MessageContent = Field()
    ident: str = Field(default="")
    type: "Message.MessageType" = Field(default=0)

class Conversation(BaseModel):
    messages: typing.List[Message] = Field(default_factory=list)

class FieldConversation(BaseModel):
# Total number of pages
    pages: int = Field(default=0)
# Max page size
    size: int = Field(default=0)
# Total number of messages
    total: int = Field(default=0)

class NestedPosition(BaseModel):
    start: int = Field(default=0)
    end: int = Field(default=0)
    page: int = Field(default=0)

class NestedListPosition(BaseModel):
    positions: typing.List[NestedPosition] = Field(default_factory=list)

class RowsPreview(BaseModel):
    class Sheet(BaseModel):
        class Row(BaseModel):
            cell: typing.List[str] = Field(default_factory=list)

        rows: typing.List[Row] = Field(default_factory=list)

    sheets: typing.Dict[str, Sheet] = Field(default_factory=dict)

class PagePositions(BaseModel):
    start: int = Field(default=0)
    end: int = Field(default=0)

class PageStructurePage(BaseModel):
    width: int = Field(default=0)
    height: int = Field(default=0)

class PageStructureToken(BaseModel):
    x: float = Field(default=0.0)
    y: float = Field(default=0.0)
    width: float = Field(default=0.0)
    height: float = Field(default=0.0)
    text: str = Field(default="")
    line: float = Field(default=0.0)

class PageStructure(BaseModel):
    page: PageStructurePage = Field()
    tokens: typing.List[PageStructureToken] = Field(default_factory=list)

class FilePages(BaseModel):
    pages: typing.List[CloudFile] = Field(default_factory=list)
    positions: typing.List[PagePositions] = Field(default_factory=list)
    structures: typing.List[PageStructure] = Field(default_factory=list)

class FileExtractedData(BaseModel):
    language: str = Field(default="")
    md5: str = Field(default="")
    metadata: typing.Dict[str, str] = Field(default_factory=dict)
    nested: typing.Dict[str, str] = Field(default_factory=dict)
    file_generated: typing.Dict[str, CloudFile] = Field(default_factory=dict)
    file_rows_previews: typing.Dict[str, RowsPreview] = Field(default_factory=dict)
    file_preview: CloudFile = Field()
    file_pages_previews: FilePages = Field()
    file_thumbnail: CloudFile = Field()
    field: str = Field(default="")
    icon: str = Field(default="")
    nested_position: typing.Dict[str, NestedPosition] = Field(default_factory=dict)
    nested_list_position: typing.Dict[str, NestedListPosition] = Field(default_factory=dict)
    title: str = Field(default="")

class LinkExtractedData(BaseModel):
    date: datetime = Field(default_factory=datetime.now)
    language: str = Field(default="")
    title: str = Field(default="")
    metadata: typing.Dict[str, str] = Field(default_factory=dict)
    link_thumbnail: CloudFile = Field()
    link_preview: CloudFile = Field()
    field: str = Field(default="")
    link_image: CloudFile = Field()
    description: str = Field(default="")
    type: str = Field(default="")
    embed: str = Field(default="")
    pdf_structure: PageStructure = Field()
# The key is the file ID
    file_generated: typing.Dict[str, CloudFile] = Field(default_factory=dict)

class ExtractedTextWrapper(BaseModel):
    _one_of_dict = {"ExtractedTextWrapper.file_or_data": {"fields": {"body", "file"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    body: ExtractedText = Field()
    file: CloudFile = Field()
    field: FieldID = Field()

class ExtractedVectorsWrapper(BaseModel):
    _one_of_dict = {"ExtractedVectorsWrapper.file_or_data": {"fields": {"file", "vectors"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    vectors: VectorObject = Field()
    file: CloudFile = Field()
    field: FieldID = Field()
    vectorset_id: str = Field(default="")

class _Dummy(BaseModel):
    """
     This message is just to trigger the utils.UserVectorsList import in protobuf_to_pydantic which does not trigger automatically when the type is inside a map
    """

    vectors: UserVectorsList = Field()

class UserVectorsWrapper(BaseModel):
    vectors: UserVectorSet = Field()
    vectors_to_delete: typing.Dict[str, UserVectorsList] = Field(default_factory=dict)# Vectorset prefix vector id
    field: FieldID = Field()

class Sentence(BaseModel):
    start: int = Field(default=0)
    end: int = Field(default=0)
    key: str = Field(default="")

class PageInformation(BaseModel):
    page: int = Field(default=0)
    page_with_visual: bool = Field(default=False)

class Representation(BaseModel):
    is_a_table: bool = Field(default=False)
    reference_file: str = Field(default="")

class ParagraphRelations(BaseModel):
    parents: typing.List[str] = Field(default_factory=list)
    siblings: typing.List[str] = Field(default_factory=list)
    replacements: typing.List[str] = Field(default_factory=list)

class Paragraph(BaseModel):
    class TypeParagraph(IntEnum):
        TEXT = 0
        OCR = 1
        INCEPTION = 2
        DESCRIPTION = 3
        TRANSCRIPT = 4
        TITLE = 5
        TABLE = 6

    start: int = Field(default=0)
    end: int = Field(default=0)
    start_seconds: typing.List[int] = Field(default_factory=list)
    end_seconds: typing.List[int] = Field(default_factory=list)
    kind: "Paragraph.TypeParagraph" = Field(default=0)
    classifications: typing.List[Classification] = Field(default_factory=list)
    sentences: typing.List[Sentence] = Field(default_factory=list)
    key: str = Field(default="")
    text: str = Field(default="")# Optional, as a computed value
    page: PageInformation = Field()
    representation: Representation = Field()
    relations: ParagraphRelations = Field()

class Position(BaseModel):
    start: int = Field(default=0)
    end: int = Field(default=0)

class Positions(BaseModel):
    position: typing.List[Position] = Field(default_factory=list)
    entity: str = Field(default="")

class FieldEntity(BaseModel):
    text: str = Field(default="")# The entity text
    label: str = Field(default="")# The entity type
    positions: typing.List[Position] = Field(default_factory=list)# The positions of the entity in the text

class FieldEntities(BaseModel):
    """
     Wrapper for a list of entities
    """

    entities: typing.List[FieldEntity] = Field(default_factory=list)

class FieldMetadata(BaseModel):
    links: typing.List[str] = Field(default_factory=list)
    paragraphs: typing.List[Paragraph] = Field(default_factory=list)
# Map of entity_text to entity_type (label) found in the text
    ner: typing.Dict[str, str] = Field(default_factory=dict)
# Map of data_augmentation_task_id to list of entities found in the field
    entities: typing.Dict[str, FieldEntities] = Field(default_factory=dict)
    classifications: typing.List[Classification] = Field(default_factory=list)
    last_index: datetime = Field(default_factory=datetime.now)
    last_understanding: datetime = Field(default_factory=datetime.now)
    last_extract: datetime = Field(default_factory=datetime.now)
    last_summary: datetime = Field(default_factory=datetime.now)
    thumbnail: CloudFile = Field()
    language: str = Field(default="")
    summary: str = Field(default="")
# Map with keys f"{entity_text}/{entity_type}" for every `entity_text` present in `ner` field, and positions as values to reflect the entity positions in the text
    positions: typing.Dict[str, Positions] = Field(default_factory=dict)
    relations: typing.List[Relations] = Field(default_factory=list)
    mime_type: str = Field(default="")

class QuestionAnswers(BaseModel):
    question_answer: typing.List[QuestionAnswer] = Field(default_factory=list)

class FieldQuestionAnswers(BaseModel):
    question_answers: QuestionAnswers = Field()
    split_question_answers: typing.Dict[str, QuestionAnswers] = Field(default_factory=dict)
    deleted_splits: typing.List[str] = Field(default_factory=list)

class FieldQuestionAnswerWrapper(BaseModel):
    _one_of_dict = {"FieldQuestionAnswerWrapper.file_or_data": {"fields": {"file", "question_answers"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    question_answers: FieldQuestionAnswers = Field()
    file: CloudFile = Field()
    field: FieldID = Field()

class FieldAuthor(BaseModel):
    """
     Who is the actor of field creation
    """
    class User(BaseModel):
        pass

    class DataAugmentation(BaseModel):
        pass

    _one_of_dict = {"FieldAuthor.author": {"fields": {"data_augmentation", "user"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    user: "FieldAuthor.User" = Field()
    data_augmentation: "FieldAuthor.DataAugmentation" = Field()

class FieldComputedMetadata(BaseModel):
    metadata: FieldMetadata = Field()
    split_metadata: typing.Dict[str, FieldMetadata] = Field(default_factory=dict)
    deleted_splits: typing.List[str] = Field(default_factory=list)

class FieldComputedMetadataWrapper(BaseModel):
    metadata: FieldComputedMetadata = Field()
    field: FieldID = Field()

class FieldText(BaseModel):
    class Format(IntEnum):
        PLAIN = 0
        HTML = 1
        RST = 2
        MARKDOWN = 3
        JSON = 4
        KEEP_MARKDOWN = 5
        JSONL = 6
        PLAIN_BLANKLINE_SPLIT = 7

    body: str = Field(default="")
    format: "FieldText.Format" = Field(default=0)
    md5: str = Field(default="")
    generated_by: FieldAuthor = Field()
    extract_strategy: str = Field(default="")

class Block(BaseModel):
    class TypeBlock(IntEnum):
        TITLE = 0
        DESCRIPTION = 1
        RICHTEXT = 2
        TEXT = 3
        ATTACHMENTS = 4
        COMMENTS = 5
        CLASSIFICATIONS = 6

    x: int = Field(default=0)
    y: int = Field(default=0)
    cols: int = Field(default=0)
    rows: int = Field(default=0)
    type: "Block.TypeBlock" = Field(default=0)
    ident: str = Field(default="")
    payload: str = Field(default="")
    file: CloudFile = Field()

class FieldLink(BaseModel):
    added: datetime = Field(default_factory=datetime.now)
    headers: typing.Dict[str, str] = Field(default_factory=dict)
    cookies: typing.Dict[str, str] = Field(default_factory=dict)
    uri: str = Field(default="")
    language: str = Field(default="")
    localstorage: typing.Dict[str, str] = Field(default_factory=dict)
    css_selector: str = Field(default="")
    xpath: str = Field(default="")
    extract_strategy: str = Field(default="")

class FieldFile(BaseModel):
    added: datetime = Field(default_factory=datetime.now)
    file: CloudFile = Field()
    language: str = Field(default="")
    password: str = Field(default="")
    url: str = Field(default="")
    headers: typing.Dict[str, str] = Field(default_factory=dict)
    extract_strategy: str = Field(default="")

class Entity(BaseModel):
    token: str = Field(default="")
    root: str = Field(default="")
    type: str = Field(default="")

class FieldLargeMetadata(BaseModel):
    entities: typing.List[Entity] = Field(default_factory=list)
    tokens: typing.Dict[str, int] = Field(default_factory=dict)

class LargeComputedMetadata(BaseModel):
    metadata: FieldLargeMetadata = Field()
    split_metadata: typing.Dict[str, FieldLargeMetadata] = Field(default_factory=dict)
    deleted_splits: typing.List[str] = Field(default_factory=list)

class LargeComputedMetadataWrapper(BaseModel):
    _one_of_dict = {"LargeComputedMetadataWrapper.file_or_data": {"fields": {"file", "real"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    real: LargeComputedMetadata = Field()
    file: CloudFile = Field()
    field: FieldID = Field()

class AllFieldIDs(BaseModel):
    """
     This message is used to store a list of all field ids of a particular
 resource. Note that title and summary fields are not included.
    """

    fields: typing.List[FieldID] = Field(default_factory=list)
