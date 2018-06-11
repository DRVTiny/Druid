# Druid

Druid is a web service which assemblies JSON representing all objects
which is direct or indirect depnedencies of the specified IT Service,
including its "associations" (hosts, hostGroups, triggers).
To be useful requires calculation engine itself which is written in perl 
and not included in this project (it is external entity, i.e. another
project). This engine will be uploaded to github soon, but now you can 
simply ask me to provide all necessary code.

## Installation

```
crystal deps && \
crystal build --release src/druid_mp.cr && \
sudo mv ./druid_mp /usr/local/bin/druid
# TODO: provide service file for systemd  
```
## Usage

TODO: Write usage instructions here

## Development

TODO: Write development instructions here

## Contributing

1. Fork it ( https://github.com/[your-github-name]/SomeRedis/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [[DRVTiny]](https://github.com/DRVTiny) Andrey Konovalov - creator, maintainer
