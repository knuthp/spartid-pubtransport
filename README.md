# Spartid Public Transport Norway


## References
1. Entur Confluence SIRI - <https://enturas.atlassian.net/wiki/spaces/PUBLIC/overview>
1.

## Personal docker fast deploy
I personally run my server on another host than i develop. During prototyping development I use a trick to quickly deploy from my developer machine.

1. Setup docker context
```
docker context create --docker "host=ssh://knuthp@ptest" ptest
```

Deploy a new version
```
docker --context ptest compose up -d --build
```
