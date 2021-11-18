const Db = require('./db.js');

(async function () {
    let result;

    await Db.setup();

    // result = await Db.add({
    //     uid: 'a',
    //     gid: 'z',
    //     tid: 'users',
    //     number: 1,
    //     boolean: true,
    //     string: 'hello world',
    // });
    // console.log('\nADD');


    // result = await Db.add({
    //     uid: 'b',
    //     gid: 'z',
    //     tid: 'users',
    //     number: 2,
    //     boolean: true,
    //     string: 'hello world'
    // });
    // console.log('\nADD');

    // result = await Db.add({
    //     uid: 'c',
    //     gid: 'z',
    //     tid: 'users',
    //     number: 3
    // });
    // console.log('\nADD');


    result = await Db.get({
        uid: 'b'
    });
    console.log('\nGET: uid - ', result);

    result = await Db.update({
        uid: 'b',
        boolean: !result.boolean,
        object: { child: { foo: 'bar' } },
        array: [ { two: 'three' }, 'one' ]
    });
    console.log('\nUPDATE: uid, boolean');

    result = await Db.query({
        gid: 'z'
    });
    console.log('\nQUERY GSI: gid - ', result);
    console.log(JSON.stringify(result, null, '\t'));

    result = await Db.query({
        gid: 'z',
        tid: 'users',
        number: 3
    });
    console.log('\nQUERY GSI: gid, tid, number - ', result);

    // result = await Db.remove({
    //     uid: 'a'
    // });
    // console.log('\nREMOVE: uid - ', result);

}()).catch(console.error);
