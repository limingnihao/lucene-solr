

带fq的测试：
```
rq={!rerank_extend firstWeight=1 reRankWeight=1 reRankJson=$rqq reRankDocs=100000 }&
rqq=
{
    "features": [
        {
            "n": "c1",
            "q": "{!func}query({!edismax q.op=or df='levels' v='${efi.levels:-}'})",
            "fq": [
                "{!edismax q.op=or df='subjects' v='${efi.subjects:-}'}"
            ]
        },
        {
            "n": "c2",
            "q": "{!func}query({!payload_score operator=or f='subjects' v='${efi.subjects:-}' func=sum})"
        },
        {
            "n": "c3",
            "q": "{!func}query({!payload_score operator=or f='subjects' v='${efi.subjects:-}' func=max})"
        },
        {
            "n": "c4",
            "q": "{!func}query({!edismax df='city' v='${efi.city:-}'})"
        },
        {
            "n": "c5",
            "q": "{!func}div(lat, lng)"
        }
    ],
    "calculate": "c1+c2+c3+c4+c5"
}

&efi.levels=专科 本科 硕士&efi.subjects=社会学 心理学 数学&efi.city=北京市

```


feature耗时测试：
```
rq={!rerank_extend firstWeight=1 reRankWeight=1 reRankJson=$rqq reRankDocs=100000 }&
rqq=
{
    "features": [
        {
            "n": "c1",
            "q": "{!func}query({!edismax q.op=or df='levels' v='${efi.levels:-}'})"
        },
        {
            "n": "c2",
            "q": "{!func}sum(query({!payload_score operator=or f='subjects' v='${efi.subject_1:-}' func=max}), query({!payload_score operator=or f='subjects' v='${efi.subject_2:-}' func=max}), query({!payload_score operator=or f='subjects' v='${efi.subject_2:-}' func=max}))"
        },
        {
            "n": "c3",
            "q": "{!func}div(lat, lng)"
        }
    ],
    "calculate": "c1+c2+c3"
}
&efi.levels=专科 本科 硕士&efi.subject_1=社会学&efi.subject_2=心理学&efi.subject_2=数学

```
