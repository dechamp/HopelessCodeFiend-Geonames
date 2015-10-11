<?php

namespace HopelessCodeFiend\Geonames\DataSource;


class DataSourceConfiguration {

    public $recover = false;
    private $file_url;
    private $data_file_within;
    private $temp_directory;
    private $DB;

	public function __construct($path_to_file, $data_file_within)
	{
		if( empty( $path_to_file ) || empty( $data_file_within ) )
		{
			throw new \InvalidArgumentException;
		}

		$this->file_url = $path_to_file;
		$this->data_file_within = $data_file_within;
	}

	public function enable_recovery()
	{
		return $this->recover = true;
	}

	public function get_file_url()
	{
		return $this->file_url;
	}

	public function get_data_file_name()
	{
		return $this->data_file_within;
	}

	public function get_temp_directory()
	{
		if( $this->temp_directory && file_exists( $this->temp_directory ) )
		{
			return $this->temp_directory;
		}

		if( empty( $this->temp_directory ) )
		{
			$this->temp_directory = '/tmp';
		}

		if( ! file_exists( $this->temp_directory ) )
		{
			mkdir( $this->temp_directory, 0777, true );
		}

		return $this->temp_directory;
	}

	public function set_temp_directory($temp_directory)
	{
		$this->temp_directory = $temp_directory;
	}

    public function get_DB()
    {
        return $this->DB;
    }

	public function set_DB( $database )
	{
		$this->DB = $database;
	}
}