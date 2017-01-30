(function() {
	'use strict';

	angular.module('app').controller('GreetingCtrl', GreetController);
	
	GreetController.$inject = ['GreetingService','$location'];

	function GreetController($service, $location) {
		var controller = this;
		var message;
		
		function get() {
			$service.get(function(greet) {
				controller.message = greet.message; 
			});
		}
		
		function configure() {
            $location.url('config');
		}
		
		controller.configure = configure;

		// Bootstrap
		get();
	}
})();