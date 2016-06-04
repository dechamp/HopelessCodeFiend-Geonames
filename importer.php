<?php
/**
 * Created by PhpStorm.
 * User: DeChamp
 * Date: 10/10/15
 * Time: 9:16 PM
 */
namespace HopelessCodeFiend\Geonames;

use Exception;
use HopelessCodeFiend\Geonames\DataSource\CountryDataSource;
use HopelessCodeFiend\Geonames\DataSource\DataSourceConfiguration;
use HopelessCodeFiend\Geonames\DataSource\ZipDataSource;
use HopelessCodeFiend\Geonames\Importer\ElasticSearchGeonamesImporter;
use HopelessCodeFiend\Geonames\Importer\MysqlGeonamesImporter;
use InvalidArgumentException;
use PDO;
use PDOException;

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


switch ($countriesOrPostalCodes) {
    case 'countries':
        $config = new DataSourceConfiguration('http://download.geonames.org/export/dump/'.$file.'.zip', ''.$file.'.txt');
        $config->setTempDirectory(dirname(__FILE__).'/tmp/countries');
        $config->enableRecovery();

        /**
         * The "Country" DataSource is for importing Countries geo information.
         *
         * You can swap this out for ZipcodesDataSource to import zip codes.
         */
        $datasource = new CountryDataSource($config);

        break;

    case 'zips':
        $config = new DataSourceConfiguration('http://download.geonames.org/export/zip/'.$file.'.zip', ''.$file.'.txt');
        $config->setTempDirectory(dirname(__FILE__).'/tmp/zips');

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

// Set the lifetime span before data resource files should be re-downloaded
$config->setMaxResourceLifeSpan(60 * 60 * 24);

/**
 * This is the actual processor to import the data.
 *
 * The only one right now is MysqlGeonamesImporter, but we can easily add
 * more such as MongoGeonamesImporter and so on.
 */
$geonamesImporter = null;

if ($dataStorageIterator === 'elasticsearch') {
    try {
        $geonamesImporter = new ElasticSearchGeonamesImporter($datasource);
    } catch (Exception $e) {
        fwrite(STDERR, $e->getMessage());
    }
} elseif ($dataStorageIterator === 'mysql') {

    try {
        $host = '127.0.0.1';
        $db = 'testdb';
        $user = 'root';
        $pass = 'root';
        $charset = 'utf8';

        $dsn = "mysql:host=$host;dbname=$db;charset=$charset";
        $opt = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ];
        $pdo = new PDO($dsn, $user, $pass, $opt);

        $config->setDatabase($pdo);
        $config->setDatabaseName('testdb');
        $geonamesImporter = new MysqlGeonamesImporter($datasource);
    } catch (PDOException $e) {
        fwrite(STDERR, 'Connection failed: '.$e->getMessage());
    } catch (Exception $e) {
        fwrite(STDERR, $e->getMessage());
    }
} else {
    fwrite(STDERR, 'Iterator '.$dataStorageIterator.' is not an option'."\n");
}

try {
    // Break up the inserts in to chucks
    $geonamesImporter->insertAtTime($database_insert_chunk_count);

    // Run the import
    $geonamesImporter->import();
} catch (Exception $e) {
    fwrite(STDERR, $e->getMessage());
}

fwrite(STDOUT, 'Processed '.$geonamesImporter->getRowsProcessedCount().' records'."\r\n");