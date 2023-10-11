## How to run:

`All instructions are to be followed within root directory (OR) directory into which the codebase is cloned`

* git clone the repository
* `npm install`
* set up the envrionment file with the name `.env`
* the `.env` file must contain the following variables set : 

*DB_USER*,
*DB_HOST*,
*DB_NAME*,
*DB_PASSWORD*

run `npm start` to execute the app.

### Note:

Database constants apart from those pertaining to connection info can be modified
within `./src/Database.ts` file, such as those pertaing to table and column names.