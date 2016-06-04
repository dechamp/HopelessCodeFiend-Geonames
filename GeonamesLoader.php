<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(-1);

// Let script run for a max of 5 hours.
set_time_limit(60 * 60 * 5);

/**
 * Composer autoloader
 *
 */
$dir = __DIR__;
require_once($dir.'/vendor/autoload.php');
