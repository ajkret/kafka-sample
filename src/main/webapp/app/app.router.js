(function() {
    'use strict';

    angular.module('app').config([ '$routeProvider', '$locationProvider', routing ]);

    function routing($routeProvider, $locationProvider) {
        $routeProvider.when('/greet', {
            templateUrl : 'app/components/greet/greet.html'
        }).when('/config', {
            templateUrl : 'app/components/config/config.html'
        }).otherwise({
            redirectTo : '/greet'
        });

        $locationProvider.html5Mode($locationProvider.html5Mode({
            enabled : true
        }));
    }

})();