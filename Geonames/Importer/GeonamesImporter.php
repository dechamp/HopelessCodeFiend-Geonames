<?php

namespace HopelessCodeFiend\Geonames\Importer;

use HopelessCodeFiend\Geonames\DataSource;
use HopelessCodeFiend\Geonames\DataSource\DataSourceBase;


abstract class GeonamesImporter {

    protected $insert_at_time = 500;
    protected $rows_to_process = 0;
    protected $report_sql = [ ];
    protected $report_params = [ ];
    protected $dataSource;
    protected $data_iterator;
    protected $DB;
    public $current_progress_file_path = '/current_progress';
    public $insert_count = 0;
    public $actual_insert_count = 0;

    public function __construct( DataSourceBase $dataSource )
    {
        $this->dataSource = $dataSource;
        $this->data_iterator = $this->dataSource->get_data_iterator();
        $this->DB = $this->dataSource->DB();
    }

    abstract public function import_to_database( \Iterator $iterator );

    public function import()
    {
        $this->import_to_database( $this->data_iterator );
    }

    public function import_count()
    {
        return $this->insert_count;
    }

    public function insert_at_time( $insert_at_time = 500 )
    {
        $this->insert_at_time = $insert_at_time;
    }

    public function __toString()
    {
        $output = '';

        foreach( $this->data_iterator AS $row_key => $row_val )
        {
            $output .= $row_key . ': ' . $this->row_to_string( $row_val ) . "\n\r<hr>";
        }

        return $output;
    }

    public function job_start()
    {
        if( ! file_exists( $this->dataSource->config->get_temp_directory() . $this->current_progress_file_path ) )
        {
            file_put_contents( $this->dataSource->config->get_temp_directory() . $this->current_progress_file_path, 0 );
        }
    }

    public function job_done()
    {
        unlink( $this->dataSource->config->get_temp_directory() . $this->current_progress_file_path );
    }

    public function update_current_progress()
    {
        file_put_contents( $this->dataSource->config->get_temp_directory() . $this->current_progress_file_path, $this->insert_count );
    }

    public function caught_up( )
    {
        $count = trim( file_get_contents( $this->dataSource->config->get_temp_directory() . $this->current_progress_file_path ) );
        return ( $this->insert_count ) >= (integer) $count ? true : false;
    }

    abstract protected function add_to_database( $data );

    private function row_to_string( $row )
    {
        $output = '';

        if( ! is_array( $row ) )
        {
            return;
        }

        foreach( $row AS $key => $val )
        {
            $output .= $key . ': ' . $val . "\n";
        }

        return $output;
    }
}