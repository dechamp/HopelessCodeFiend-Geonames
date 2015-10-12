<?php
/**
 * Created by PhpStorm.
 * User: DeChamp
 * Date: 7/22/15
 * Time: 7:07 PM
 */

namespace HopelessCodeFiend\Geonames\DataSource;


use HopelessCodeFiend\Geonames\Iterator\FileIterator;
use ZipArchive;

class DataSourceBase {

    public $DB;
    public $table;
    public $config;
    protected $file_name;
    protected $temp_file_path;
    protected $temp_file;
    protected $parsed_data;
    protected $mapped_columns;
    protected $unique_keys = [ ];

    public function __construct( DataSourceConfiguration $config )
    {

        /**
         *  Set the database table column names
         *
         * Set within the class extending this class, as an attribute
         */
        if( count( $this->mapped_columns ) < 1 )
        {
            throw new \Exception( 'You must set the mapped_columns attribute' );
        }

        /**
         *  Set the database table name
         *
         * Set within the class extending this class, as an attribute
         */
        if( empty( $this->table ) )
        {
            throw new \Exception( 'You must set the tables attribute' );
        }

        $this->config = $config;
        $this->file_name = basename( $this->config->get_file_url() );
        $this->temp_file_path = $this->config->get_temp_directory();
        $this->temp_file = $this->config->get_temp_directory() . '/' . md5( $this->file_name . date( 'h' ) ) .'.zip';
        $this->DB = $config->get_DB();
    }

    public function DB()
    {
        return $this->DB;
    }

    public function get_mapped_columns()
    {
        return $this->mapped_columns;
    }

    public function get_unique_keys()
    {
        return $this->unique_keys;
    }

    /**
     * Retrieve the datafile file set within the datasource config model
     *
     * @return string
     */
    private function get_data_file_path()
    {
        return $this->config->get_temp_directory() . '/' . $this->config->get_data_file_name();
    }

    /**
     * return the parsed data
     *
     * @return string
     */
    public function pull_data()
    {
        if( ! isset( $this->parsed_data ) )
        {
            $this->parsed_data = $this->get_data_iterator();
        }

        return $this->parsed_data;
    }

    public function get_data_iterator()
    {
        return $this->get_processed_data_iterator();
    }

    public function process_row( $value )
    {
        if( count( $this->mapped_columns ) === count( $value ) )
        {
            return array_combine( $this->mapped_columns, $value );
        }
    }

    protected function get_processed_data_iterator()
    {
        $results = $this->process_data_to_iterator();

        return $results;
    }

    /**
     * Parse the raw data to rows array
     *
     * @return array
     */
    protected function process_data_to_iterator()
    {
        $file = $this->get_zip_file();

        return new FileIterator( $file );
    }

    /**
     * Gets remote file, returns datafile set within the DataSourceConfig model
     *
     * @return string
     * @throws \Exception
     */
    protected function get_zip_file()
    {
        try
        {
            if( ! file_exists( $this->get_data_file_path() ) || ( filectime( $this->get_data_file_path() ) + 60 * 60 ) <= time() )
            {
                $this->grab_remote_file();
                $this->unzip();
            }

            return $this->get_data_file_path();

        }
        catch( Exception $e )
        {
            echo $e->getMessage();
        }
    }

    /**
     * Unzips file and stores all files in temporary file
     *
     * @throws \Exception
     */
    private function unzip()
    {

        // Extract the zip file
        try
        {
            $zip = new ZipArchive;
            chmod( $this->temp_file, 0777 );

            if( $res = $zip->open( $this->temp_file ) !== true )
            {
                switch($res){
                    case ZipArchive::ER_EXISTS:
                        $ErrMsg = "File already exists.";
                        break;

                    case ZipArchive::ER_INCONS:
                        $ErrMsg = "Zip archive inconsistent.";
                        break;

                    case ZipArchive::ER_MEMORY:
                        $ErrMsg = "Malloc failure.";
                        break;

                    case ZipArchive::ER_NOENT:
                        $ErrMsg = "No such file.";
                        break;

                    case ZipArchive::ER_NOZIP:
                        $ErrMsg = "Not a zip archive.";
                        break;

                    case ZipArchive::ER_OPEN:
                        $ErrMsg = "Can't open file.";
                        break;

                    case ZipArchive::ER_READ:
                        $ErrMsg = "Read error.";
                        break;

                    case ZipArchive::ER_SEEK:
                        $ErrMsg = "Seek error.";
                        break;

                    default:
                        $ErrMsg = "Unknown";
                        break;


                }

                throw new \Exception( 'Unable to process zip file ' . $this->temp_file . ' :: ' . $ErrMsg );
            }

            $zip->extractTo( $this->config->get_temp_directory() );
            $zip->close();

        }
        catch( \Exception $e )
        {
            echo $e->getMessage();
        }
    }

    /**
     * Grabs file from url
     *
     * @return mixed
     */
    private function grab_remote_file()
    {
        file_put_contents( $this->temp_file_path . '/progress.txt', '' );
        $curl = curl_init( $this->config->get_file_url() );
        curl_setopt( $curl, CURLOPT_RETURNTRANSFER, 1 );
        curl_setopt( $curl, CURLOPT_NOPROGRESS, 0 );
        curl_setopt( $curl, CURLOPT_PROGRESSFUNCTION, [ $this, 'progress_callback' ]);
        $grabbed_file = curl_exec( $curl );
        file_put_contents( $this->temp_file, $grabbed_file );
    }

    /**
     * Get process of the download. This was provided from https://gist.github.com/bdunogier/1030450
     *
     */
    private function progress_callback( $resource, $download_size, $downloaded_size, $upload_size, $uploaded_size = null )
    {
        static $previousProgress = 0;

        if( $download_size == 0 )
        {
            $progress = 0;
        }
        else
        {
            $progress = round( $downloaded_size * 100 / $download_size );
        }

        if( $progress > $previousProgress )
        {
            if( $previousProgress === 0 )
            {
                echo 'Progress...' . "\r\n";
            }

            $previousProgress = $progress;
            @file_put_contents( $this->temp_file_path . '/progress.txt', $progress . "\n", FILE_APPEND );
            echo $progress . "%\r\n";

        }
    }

}