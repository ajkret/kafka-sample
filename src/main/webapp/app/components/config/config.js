(function() {
    'use strict';

    angular.module('app').controller('ConfigCtrl', ConfigCtrl);

    ConfigCtrl.$inject = [ 'GreetingService', '$location' ];

    function ConfigCtrl($service, $location) {
        var controller = this;
        var message;
        var errorMessage="";

        function get() {
            $service.get(function(greet) {
                controller.message = greet.message;
            });
        }

        function save() {
            $service.save({
                message : controller.message
            }, function() {
                controller.mainPage();
                controller.errorMessage="";
            }, function(data) {
                controller.errorMessage = "Problem saving Message of the Day";
            });
        }

        function mainPage() {
            $location.url('/');
        }

        controller.save = save;
        controller.mainPage = mainPage;

        // Bootstrap
        get();
    }

})();