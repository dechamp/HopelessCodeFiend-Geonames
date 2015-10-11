<?php

namespace HopelessCodeFiend\Geonames\Importer;

use HopelessCodeFiend\Geonames\DataSource;

class MysqlGeonamesImporter extends GeonamesImporter {

    private $force_run = false;

    public function import_to_database(\Iterator $iterator )
	{
        try
        {
            $j = 0;

            self::job_start();

	        while( ( $row = $iterator->current() ) !== false )
	        {

                $this->report_sql[] = '
                    INSERT INTO
                      tb_data.' . $this->dataSource->table . '
                    SET ' . $this->map_columns( $iterator->key() ) . '
                    ON DUPLICATE KEY UPDATE
                       ' . $this->map_columns( $iterator->key(), 1 )
                ;

                $this->report_params = array_merge( $this->report_params, $this->map_params( $iterator->key() ) );

                $j++;
                $iterator->delete();

                $this->add_to_database( $j );
            }

            if( count( $this->report_sql ) > 0 )
            {
                $this->force_run = true;
                $this->add_to_database( $j );
            }

            self::job_done();

        }
        catch( \Exception $e )
        {
            throw $e;
        }
	}

    protected function add_to_database( $j )
    {
        $this->insert_count += count( $this->report_sql );

        if( $this->dataSource->config->recover === true && ! self::caught_up() )
        {
            $this->report_sql = [ ];
            $this->report_params = [ ];
            return null;
        }

        if( $j >= $this->insert_at_time || $this->force_run === true )
        {

            $res = call_user_func(
                [
                    $this->DB,
                    'Execute'
                ], implode( ';', $this->report_sql ), $this->report_params
            );

            if( $res[ 'error' ] )
            {
                die( var_export( $res ) );
            }

            $this->actual_insert_count += count( $this->report_sql );
            $this->update_current_progress();
            echo $this->actual_insert_count . ' inserted' . "\r\n";

            $this->report_sql = [ ];
            $this->report_params = [ ];
            $j = 0;
            $this->force_run = false;
        }
    }

    protected function map_columns( $count, $duplicate = null )
    {
        $columns = $this->dataSource->get_mapped_columns();
        $unique_keys = $this->dataSource->get_unique_keys();

        if( isset( $duplicate ) )
        {
            foreach( $unique_keys AS $primary_key )
            {
                unset( $columns[ $primary_key ] );
            }
        }

        $results = '';

        foreach( $columns AS $column )
        {
            $results .= $column . ' = {{' . $column . '_' . $count . '}},';
        }

        return rtrim( $results, ',' );
    }

	protected function map_params( $key )
	{
		$columns = $this->dataSource->get_mapped_columns();
		$report_params = [ ];
		$row = $this->data_iterator->current();

		// Check for invalid rows
		if( count( $row ) !== count( $columns ) )
		{
			error_log( 'Invalid row: ' . $this->actual_insert_count . ' :: ' . $row . "\n" );
			echo 'line ' . $this->actual_insert_count . ' is invalid and was skipped' . "\n";
			return null;
		}

		foreach( $columns AS $column_key => $column_val )
		{
			$report_params[ $column_val . '_' . $key ] = $row[ $column_key ];
		}

		return $report_params;
	}
}