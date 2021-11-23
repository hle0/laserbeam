function *_database(table) {
    let i = Deno.core.opSync("database.begin", table);
    while (1) {
        let v = Deno.core.opSync("database.next", i);
        if (v) {
            yield v;
        } else {
            break;
        }
    }
}

function sendOne(item) {
    Deno.core.opSync("database.send", [item]);
}

function send(stuff) {
    for (let item of stuff) {
        sendOne(item);
    }
}

function _genWrapper(gen) {
    return Object.assign(gen, {
        map: function (f) {
            let prev = this;
            return _genWrapper(function *() {
                for (let item of prev) {
                    yield f(item);
                }
            }());
        },
        filter: function (f) {
            let prev = this;
            return _genWrapper(function *() {
                for (let item of prev) {
                    if (f(item)) {
                        yield item;
                    }
                }
            }());
        },
        fold: function (f, initial) {
            let result = initial;
            for (let item of this) {
                result = f(initial, result);
            }
            return result;
        },
        enumerate: function () {
            let prev = this;
            return _genWrapper(function *() {
                let i = 0;
                for (let item of prev) {
                    yield [i, item];
                    i++;
                }
            }());
        },
        collect: function() {
            return Array.from(this);
        },
        forEach: function(f) {
            return this.map(f).map(_ => undefined).fold((_, _) => undefined);
        },
        sendAll: function() {
            return this.forEach(sendOne);
        }
    });
}

const database = (which) => _genWrapper(_database(which));