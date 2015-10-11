<?php

namespace HopelessCodeFiend\Geonames\DataSource;

class ZipDataSource extends DataSourceBase {

    public $table = 'geonames_zips';

    protected $unique_keys = ['id'];

	protected $mapped_columns = [
		'country_code',
        'postal_code',
        'place_name',
        'admin_name1',
        'admin_code1',
        'admin_name2',
        'admin_code2',
        'admin_name3',
        'admin_code3',
        'latitude',
        'longitude',
        'accuracy'
	];
}
