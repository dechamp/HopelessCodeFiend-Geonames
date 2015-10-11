<?php

namespace HopelessCodeFiend\Geonames\DataSource;

class CountryDataSource extends DataSourceBase {

    public $table = 'geonames';

    protected $unique_keys = ['geonameid'];

	protected $mapped_columns = [
		'geonameid',
		'name',
		'asciiname',
		'alternatenames',
		'latitude',
		'longitude',
		'feature_class',
		'feature_code',
		'country_code',
		'cc2',
		'admin1_code',
		'admin2_code',
		'admin3_code',
		'admin4_code',
		'population',
		'elevation',
		'dem',
		'timezone',
		'modification_date'
	];

}