const Db = require('./db.js');

(async function () {

    const item = {
        uid: 'a'
    };

    const option = {
        condition: {
            tid: 'tid'
        }
    };

    await Db.setup();
    await Db.remove(item, option);

    console.log('REMOVE: should throw error');

}()).catch(console.error);
