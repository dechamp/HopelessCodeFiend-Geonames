<?php
/**
 * Created by PhpStorm.
 * User: DeChamp
 * Date: 7/22/15
 * Time: 7:07 PM
 */

namespace HopelessCodeFiend\Geonames\DataSource;


use Exception;
use HopelessCodeFiend\Geonames\Iterator\FileIterator;
use ZipArchive;

class DataSourceBase
{

    public $DB;
    public $table;
    public $config;
    protected $fileName;
    protected $tempFilePath;
    protected $tempFile;
    protected $parsedData;
    protected $mappedColumns;
    protected $uniqueKeys = [];

    public function __construct(DataSourceConfiguration $config)
    {

        /**
         *  Set the database table column names
         *
         * Set within the class extending this class, as an attribute
         */
        if (count($this->mappedColumns) < 1) {
            throw new Exception('You must set the mapped_columns attribute');
        }

        /**
         *  Set the database table name
         *
         * Set within the class extending this class, as an attribute
         */
        if (empty($this->table)) {
            throw new Exception('You must set the tables attribute');
        }

        $this->config = $config;
        $this->fileName = basename($this->config->getFileUrl());
        $this->tempFilePath = $this->config->getTempDirectory();
        $this->tempFile = $this->config->getTempDirectory().'/'.$this->fileName;
        $this->DB = $config->getDB();
    }

    public function DB()
    {
        return $this->DB;
    }

    public function getMappedColumns()
    {
        return $this->mappedColumns;
    }

    public function getUniqueKeys()
    {
        return $this->uniqueKeys;
    }

    /**
     * Retrieve the datafile file set within the datasource config model
     *
     * @return string
     */
    private function getDataFilePath()
    {
        return $this->config->getTempDirectory().'/'.$this->config->getDataFileName();
    }

    /**
     * return the parsed data
     *
     * @return string
     */
    public function pullData()
    {
        if (!isset($this->parsedData)) {
            $this->parsedData = $this->getDataIterator();
        }

        return $this->parsedData;
    }

    public function getDataIterator()
    {
        return $this->getProcessedDataIterator();
    }

    public function processRow($value)
    {
        if (count($this->mappedColumns) === count($value)) {
            return array_combine($this->mappedColumns, $value);
        }

        return null;
    }

    protected function getProcessedDataIterator()
    {
        $results = $this->processDataToIterator();

        return $results;
    }

    /**
     * Parse the raw data to rows array
     *
     * @return array
     */
    protected function processDataToIterator()
    {
        $file = $this->getZipFile();

        // TODO convert to injection over dependancy
        return new FileIterator($file);
    }

    /**
     * Gets remote file, returns datafile set within the DataSourceConfig model
     *
     * @return string
     * @throws Exception
     */
    protected function getZipFile()
    {
        try {
            if (!file_exists($this->getDataFilePath()) || (filectime($this->getDataFilePath()) + 60 * 60) <= time()) {
                $this->emptyTmpFolder();
                $this->grabRemoteFile();
                $this->unzip();
            }

            return $this->getDataFilePath();

        } catch (Exception $e) {
            fwrite(STDERR, $e->getMessage());
        }
    }

    /**
     * Unzips file and stores all files in temporary file
     *
     * @throws Exception
     */
    private function unzip()
    {

        // Extract the zip file
        try {
            $zip = new ZipArchive;
            chmod($this->tempFile, 0777);

            if (($res = $zip->open($this->tempFile, ZipArchive::OVERWRITE)) !== true) {
                switch ($res) {
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

                throw new Exception('Unable to process zip file '.$this->tempFile.' :: '.$ErrMsg);
            }

            $zip->extractTo($this->config->getTempDirectory());
            $zip->close();

        } catch (Exception $e) {
            fwrite(STDERR, $e->getMessage());
        }
    }

    /**
     * Grabs file from url
     *
     * @return mixed
     */
    private function grabRemoteFile()
    {
        file_put_contents($this->tempFilePath.'/progress.txt', '');
        $curl = curl_init($this->config->getFileUrl());
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_NOPROGRESS, 0);
        curl_setopt($curl, CURLOPT_PROGRESSFUNCTION, [$this, 'progressCallback']);
        $grabbed_file = curl_exec($curl);
        file_put_contents($this->tempFile, $grabbed_file);
    }

    private function emptyTmpFolder()
    {
        $tmp_path = rtrim($this->tempFilePath,'/');
        if(file_exists($tmp_path)) {
            $files = glob($tmp_path.'/*');
            foreach ($files as $file) {
                if (is_file($file)) {
                    unlink($file); // delete file
                }
            }
        }
    }

    /**
     * Get process of the download. This was provided from https://gist.github.com/bdunogier/1030450
     *
     */
    private function progressCallback($resource, $downloadSize, $downloadedSize, $upload_size, $uploaded_size = null)
    {
        static $previousProgress = 0;

        if ($downloadSize == 0) {
            $progress = 0;
        } else {
            $progress = round($downloadedSize * 100 / $downloadSize);
        }

        if ($progress > $previousProgress) {
            if ($previousProgress === 0) {
                fwrite(STDOUT, 'Progress...'."\r\n");
            }

            $previousProgress = $progress;
            @file_put_contents($this->tempFilePath.'/progress.txt', $progress."\n", FILE_APPEND);
            fwrite(STDOUT, $progress."%\r\n");
        }
    }

}