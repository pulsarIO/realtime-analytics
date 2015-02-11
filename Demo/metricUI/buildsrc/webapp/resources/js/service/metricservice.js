angular.module("indexApp").factory('MetricService', ['$resource', function ($resource) {
   "use strict";
   return $resource('http://10.64.254.178:8083/pulsar/metric?:params' + '', {params:null}, {query: {method: 'GET', cache:false, isArray:true}});
}]);