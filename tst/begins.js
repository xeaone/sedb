const Sedb = require('../index.js');

(async function () {
    let result, pk, sk;

    const Db = new Sedb.dynamo({
        name: 'sedb',
        region: 'us-west-2',
        version: '2012-08-10',
        query: { begins: true },
        primary: { hash: 'pk', range: 'sk' }
    });

    await Db.setup();

    sk = 'item_id_0_2';
    pk = 'account_organization';
    result = await Db.add({ pk, sk, foo: 'bar' });
    console.log('\nADD: ', sk);

    sk = 'item_id_1_1';
    pk = 'account_organization';
    result = await Db.add({ pk, sk, baz: 'bat' });
    console.log('\nADD: ', sk);

    sk = 'item_id';
    pk = 'account_organization';
    result = await Db.query({ pk, sk });
    console.log('\nQUERY: ', result);

}()).catch(console.error);
