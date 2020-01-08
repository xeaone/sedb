const Db = require('./db.js');

(async function () {

    const item = {
        uid: 'a'
    };

    await Db.setup();
    await Db.remove(item);

    console.log('REMOVE: should remove item with uid=a');

}()).catch(console.error);
