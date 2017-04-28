/**
 * Created by spin on 4/22/17.
 */

"use strict";

const Promise = require('bluebird');

Promise.resolve()
    .then(function(){
        throw new Error('hey');
    })
    .catch(function(e){
        console.error(e);
    })
    .then(function(res){
        console.log(res);
    })