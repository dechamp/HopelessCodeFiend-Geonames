<?php
/**
 * Created by PhpStorm.
 * User: DeChamp
 * Date: 10/10/15
 * Time: 9:16 PM
 */
namespace HopelessCodeFiend;

use HopelessCodeFiend\Geonames\DataSource\DataSourceConfiguration;
use HopelessCodeFiend\Geonames\DataSource\CountryDataSource;
use HopelessCodeFiend\Geonames\DataSource\ZipDataSource;
use HopelessCodeFiend\Geonames\Importer\ElasticSearchGeonamesImporter;
use HopelessCodeFiend\Geonames\Importer\MysqlGeonamesImporter;
use InvalidArgumentException;
use PDO;

// CLI Options
$short_options = "";
$short_options .= "t:"; // countries or zips
$short_options .= "z::"; // Zip file name, default is US
$short_options .= "d::"; // Data storage (elasticsearch[default], mysql)
$short_options .= "c::"; // Database insert chunk count, defaults to 500
$long_options = [
    "debug",
];

$options = getopt($short_options, $long_options);

$countriesOrPostalCodes = array_key_exists('t', $options) ? $options['t'] : 'countries';
$file = array_key_exists('z', $options) ? $options['z'] : 'US';
$dataStorageIterator = array_key_exists('d', $options) ? $options['d'] : 'elasticsearch';
$database_insert_chunk_count = array_key_exists('c', $options) ? $options['c'] : 500;
$_DEBUG = array_key_exists('debug', $options) ? $options['debug'] : false;

if ($countriesOrPostalCodes !== 'countries' && $countriesOrPostalCodes !== 'zips') {
    throw new InvalidArgumentException('-t="countries" or -t="zips" required');
}

require_once('GeonamesLoader.php');

if (!$_DEBUG) {
    ob_start();
}

/**
 * Geonames processor
 *
 * This provides the ability to import all states and zips for
 * what ever location you want by configuring the settings below.
 */

fwrite(STDOUT, 'Starting Import...'."\r\n");

$config = new DataSourceConfiguration('http://download.geonames.org/export/dump/'.$file.'.zip', ''.$file.'.txt');

switch ($countriesOrPostalCodes) {
    case 'countries':
        $config->setTempDirectory('tmp/countries');
        $config->enableRecovery();

        /**
         * The "Country" DataSource is for importing Countries geo information.
         *
         * You can swap this out for ZipcodesDataSource to import zip codes.
         */
        $datasource = new CountryDataSource($config);

        break;

    case 'zips':
        $config->setTempDirectory('tmp/zips');

        /**
         * The "Country" DataSource is for importing Countries geo information.
         *
         * You can swap this out for ZipcodesDataSource to import zip codes.
         */
        $datasource = new ZipDataSource($config);

        break;

    default:
        fwrite(STDERR, 'Please choose either "countries" or "zips"');
        exit();
}

/**
 * This is the actual processor to import the data.
 *
 * The only one right now is MysqlGeonamesImporter, but we can easily add
 * more such as MongoGeonamesImporter and so on.
 */
if ($dataStorageIterator === 'elasticsearch') {
    $geonamesImporter = new ElasticSearchGeonamesImporter($datasource);
} elseif ($dataStorageIterator === 'mysql') {
    
    // $Config is passed to $datasource above
    $config->setDB(new PDO('mysql:host=localhost;dbname=testdb;charset=utf8mb4', 'root', 'root'));
    $geonamesImporter = new MysqlGeonamesImporter($datasource);
} else {
    die(' Iterator '.$dataStorageIterator.' is not an option');
}

$geonamesImporter->insertAtTime($database_insert_chunk_count);
$geonamesImporter->import();

fwrite(STDOUT, 'Imported '.$geonamesImporter->importCount().' records'."\r\n");