(function() {
    'use strict';

    angular.module('app').factory('GreetingService', GreetingService);

    GreetingService.$inject = ['$resource'];

    function GreetingService($resource) {
        return $resource('rest/greet', {
            get : {
                method : 'GET',
                timeout : 10000,
                isArray : false
            },
            save : {
                method : 'POST',
                timeout : 10000
            }
        });
    }
})();