async function *database() {
    let i = await Deno.core.opAsync("database.begin");
    while (await Deno.core.opAsync("database.more", i)) {
        [i, batch] = await Deno.core.opAsync("database.next", i);
        yield *(batch.map(x => JSON.parse(x)));
    }
}

async function send_one(item) {
    await Deno.core.opAsync("database.send", [item]);
}

async function send(stuff) {
    for await (let item of stuff) {
        await send_one(item);
    }
}