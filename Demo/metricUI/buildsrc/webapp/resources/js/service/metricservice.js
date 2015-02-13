angular.module("indexApp").factory('MetricService', ['$resource', function ($resource) {
   "use strict";
   return $resource('http://'+location.host+'/pulsar/metric?:params' + '', {params:null}, {query: {method: 'GET', cache:false, isArray:true}});
}]);