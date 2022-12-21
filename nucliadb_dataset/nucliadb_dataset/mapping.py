# Mapping to transform a text/labels onto tokens and multilabelbinarizer
from typing import Any, Dict, List, Tuple

from sklearn.preprocessing import MultiLabelBinarizer
from nucliadb_protos.train_pb2 import Label
from transformers import PreTrainedTokenizer

# Mapping
def text_label_to_list():
    def func(X: str, Y: List[Label]) -> Tuple[str, List[str]]:
        Y = [f"/l/{label.labelset}/{label.label}" for label in Y]
        return X, Y

    return func


def tokenize(tokenizer: PreTrainedTokenizer):
    def func(X: str, Y: Any) -> Tuple[Any, Any]:
        X = tokenizer(X)
        return X, Y

    return func


def encoder(encoder):
    def func(X: Dict[str, Any], Y: Any) -> Tuple[Any, Any]:
        # X is tokenized
        X = encoder(X)
        return X, Y

    return func


def mlb(mlb: MultiLabelBinarizer):
    def func(X: Any, Y: List[str]) -> Tuple[Any, Any]:
        Y = mlb.fit_transform(Y)
        return X, Y

    return func


def sumarize_text(sumarizer):
    def func(X: str, Y: Any) -> Tuple[str, Any]:
        X = sumarizer(X)
        return X, Y

    return func
