const Db = require('./db.js');

(async function () {

    const item = {
        uid: 'a'
    };

    const option = {
        condition: {
            tid: 'users'
        }
    };

    await Db.setup();
    await Db.remove(item, option);

    console.log('REMOVE: should remove item');

}()).catch(console.error);
