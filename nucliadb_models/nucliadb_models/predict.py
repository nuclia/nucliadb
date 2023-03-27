from enum import Enum


class SemanticModel(str, Enum):
    ENGLISH = "en"
    MULTILINGUAL = "multilingual-2023-02-21"


class AnonimizationModel(str, Enum):
    DISABLED = "disabled"
    MULTILINGUAL = "multilingual"


class GenerativeModel(str, Enum):
    MULTILINGUAL = "generative-multilingual-2023"
    CHATGPT = "chatgpt"


class NERModel(str, Enum):
    MULTILINGUAL = "multilingual"
