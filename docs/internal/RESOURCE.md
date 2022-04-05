# Resource

Example of resource message:

```json
{
    "resource": "9c66c1c9-09ff-45da-aaa1-25c12bc9664a",
    "metadata": {
        "origin": 
        "slug": "documento-con-minusculas"
        "modified":  0001-01-01T00:00:00Z
        "created":  0001-01-01T00:00:00Z
        "order": 1
        "sentences": 3
        "questions": [3,4,5]
        "type":
    },

    "text": {
        "body": "A body of a document",
        "title": "The title of the document",
    },

    "vectors": {
        "body": [
                {
                    "sentence": 1,
                    "vector": b"092340928029348"
                },
                {
                    "sentence": 2,
                    "vector": b"2342439874397478"
                },
            ],
        "title": [
            {
                "sentence": 1,
                "vector": b"645f756d98a39b9"
            }
        ]
    },

    "status": 0, // USEFUL
    "paragraphs": [
        {
            "field": "body",
            "start: 0,
            end": 5,
            "label": ["cars", "fancy", "brand"],
        },
        {
            "field": "body",
            "start": 5,
            "end": 10,
            "label": ["cars", "fancy", "interior"],
        },
    ],
    "sentences": [

        {
            "field": "body",
            "start": 0,
            "end": 5,
            "label": ["cars", "fancy", "brand"],
        },
        {
            "field": "body",
            "start": 5,
            "end": 10,
            "label": ["cars", "fancy", "interior"],
        },
    ],

    "labels": [
        {
            "label": "cars",
            "user": false
        },
        {
            "label": "myFavouriteCar",
            "user": true
        }
    ]
}   
```