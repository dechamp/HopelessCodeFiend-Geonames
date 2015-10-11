<?php
namespace HopelessCodeFiend\Geonames\Iterator;

class FileIterator implements \Iterator {

	private $position = 0;
	private $highest_pull = 0;
	private $file_handle;
	private $file_to_parse;
	private $data = [];
	private $rewind_called = false;

	public function __construct( $file_to_parse )
	{
        try
        {

            if( ! file_exists( $file_to_parse ) )
            {
                throw new \Exception( 'File does not exists' );
            }

            $this->file_to_parse = $file_to_parse;

            if( ( $this->file_handle = @fopen( $this->file_to_parse, "r" ) ) === false )
            {
                throw new \Exception( 'no data in file to parse' );
            }
        }
        catch( \Exception $e )
        {
            echo $e->getMessage();
        }
	}

	public function __destruct()
	{
        if( isset( $this->file_handle ) )
        {
            fclose( $this->file_handle );
        }
	}

	public function key()
	{
		return $this->position;
	}

	public function current()
	{
		if( $this->position === 0 && $this->rewind_called !== true )
		{
			$this->pull_data();
		}

		if( $this->rewind_called === true )
		{
			$this->rewind_called = false;
		}

		if( ! array_key_exists( $this->position, $this->data ) )
		{
			return false;
		}

		return $this->data[ $this->position ];
	}

	public function valid()
	{
		return isset( $this->data[ $this->position ] );
	}

	public function next()
	{
		++ $this->position;
		$this->pull_data();
	}

	public function last()
	{
		-- $this->position;
	}


	public function delete()
	{
		unset( $this->data[ $this->position ] );
		$this->next();
	}

	public function rewind()
	{
		$this->rewind_called = true;

		if( $this->position === 0 )
		{
			$this->pull_data();
			return true;
		}

		$this->position = 0;
	}

	private function pull_data()
	{
		/**
		 * If data already exist for position, then skip pulling from file
		 * and return existing data
		 */
		if( $this->position <= $this->highest_pull && count( $this->data ) > 0 )
		{
			return $this->data[ $this->position ];
		}

		/**
		 * If we are pulling data that hasn't already been set then
		 * increase the highest pull number and move forward with pulling new data
		 */
		if( $this->position > $this->highest_pull )
		{
			$this->highest_pull = $this->position;
		}

		/**
		 * Make sure we have reached the end of the file
		 */
		if( feof( $this->file_handle ) )
		{
			return false;
		}

		/**
		 * Make sure we are returning data being requested
		 */
		if( ( $data = fgets( $this->file_handle ) ) === false )
		{
			return false;
		}

        $data = explode( "\t", $data );

		/**
		 * Assign and return the new row after sanatizing it
		 */
		return $this->data[ $this->position ] = array_map("utf8_encode", $data );
	}
}

