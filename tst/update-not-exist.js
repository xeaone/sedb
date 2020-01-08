const Db = require('./db.js');

(async function () {

    const item = {
        uid: 'z',
        text: 'hello wolrd'
    };

    await Db.setup();
    await Db.update(item);

    console.log('UPDATE: not exist should throw');

}()).catch(console.error);
