# NUCLIADB TRAIN CLIENT

Library to connect NucliaDB to Training APIs

## INSTALL

```
pip install nucliadb_dataset
```

## USE IT

```

from nucliadb_dataset import get_sentences

for sentence in get_sentences("123123213213", labels=True, entities=True, text=True):
    ...
```

```
from nucliadb_dataset.api import get_labels

labels = get_labels("ce560fb1-6395-428d-b872-65cd08b5511b")

```
