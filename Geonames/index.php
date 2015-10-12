<?php
/**
 * Created by PhpStorm.
 * User: DeChamp
 * Date: 10/10/15
 * Time: 9:16 PM
 */

$countries_or_postal_codes  = isset( $argv[1] ) ? $argv[1] : '';
$file  = isset( $argv[2] ) ? $argv[2] : 'US';
$data_storage_iterator  = isset( $argv[3] ) ? $argv[3] : 'elasticsearch';
$database_insert_chunk_count  = isset( $argv[4] ) ? $argv[4] : '';
$_DEBUG = isset( $argv[5] ) ? $argv[5] * 1.0 : 0;

if( empty( $countries_or_postal_codes ) )
{
    die( 'Use: php ' . $_SERVER['PHP_SELF'] . ' <"countries" or "zips", required>, <zip_file_name, optional, default: US>, <database_insert_chuck_count, optional, default:500>, <data_storage_iterator, optional, default:elasticsearch, options(elasticsearch, mysql)>, <debug, optional, default:0>' );
}

require_once('GeonamesLoader.php');

if( ! $_DEBUG )
{
    ob_start();
}

/**
 * Geonames processor
 *
 * This provides the ability to import all states and zips for
 * what ever location you want by configuring the settings below.
 */
use HopelessCodeFiend\Geonames\DataSource\DataSourceConfiguration;
use HopelessCodeFiend\Geonames\DataSource\CountryDataSource;
use HopelessCodeFiend\Geonames\DataSource\ZipDataSource;
use HopelessCodeFiend\Geonames\Importer\ElasticSearchGeonamesImporter;
use HopelessCodeFiend\Geonames\Importer\MysqlGeonamesImporter;

echo 'Starting Import...' . "\r\n";

switch( $countries_or_postal_codes )
{
    case 'countries':
        $Config = new DataSourceConfiguration( 'http://download.geonames.org/export/dump/' . $file . '.zip', '' . $file . '.txt' );
        $Config->set_temp_directory( 'tmp/countries' );
        $Config->enable_recovery();

        /**
         * The "Country" DataSource is for importing Countries geo information.
         *
         * You can swap this out for ZipcodesDataSource to import zip codes.
         */
        $datasource = new CountryDataSource( $Config );

        break;

    case 'zips':
        $Config = new DataSourceConfiguration( 'http://download.geonames.org/export/zip/' . $file . '.zip', '' . $file . '.txt' );
        $Config->set_temp_directory( 'tmp/zips' );

        /**
         * The "Country" DataSource is for importing Countries geo information.
         *
         * You can swap this out for ZipcodesDataSource to import zip codes.
         */
        $datasource = new ZipDataSource( $Config );

        break;

    default:
        die('Please choose either "countries" or "zips"');
}

/**
 * This is the actual processor to import the data.
 *
 * The only one right now is MysqlGeonamesImporter, but we can easily add
 * more such as MongoGeonamesImporter and so on.
 */
if($data_storage_iterator === 'elasticsearch')
{
    $geonames_importer = new ElasticSearchGeonamesImporter( $datasource );
}
elseif($data_storage_iterator === 'mysql')
{
    $geonames_importer = new MysqlGeonamesImporter( $datasource );
}
else
{
    die(' Iterator ' . $data_storage_iterator . ' is not an option');
}

$geonames_importer->insert_at_time( $database_insert_chunk_count );
$geonames_importer->import();

echo 'Imported ' . $geonames_importer->import_count() . ' records' . "\r\n";