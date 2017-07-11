<?php
/**
 * @author Sergio Bertolin <sbertolin@owncloud.com>
 *
 * @copyright Copyright (c) 2017, ownCloud GmbH
 * @license AGPL-3.0
 *
 * This code is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License, version 3,
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *
 */

namespace OCA\Testing;

use OCP\IDBConnection;

/**
 * Class for increasing file ids over 32 bits max int
 */
class BigFileID {

	/** @var IDBConnection */
	private $connection;

	public function __construct(IDBConnection $connection) {
		$this->connection = $connection;
	}

	/*Put a dummy entry to make the autoincrement go beyond the 32 bits limit*/
	public function increaseFileIDsBeyondMax32bits(){
		//public function executeQuery($query, array $params = [], $types = []);
		$query = "INSERT INTO oc_filecache (fileid, storage, path, path_hash, parent, name, mimetype, mimepart, size, mtime, storage_mtime, encrypted, unencrypted_size, etag, permissions, checksum) VALUES ('2147483647', '10000', 'files', '59f91d3e7ebd97ade2e147d2066cc4eb', '5831', '', '4', '3', '163', '1499256550', '1499256550', '0', '0', '595cd6e63f375', '27', 'NULL')";    
		$this->connection->executeQuery($query);	
	}

	/*Remove the previously added dummy entry*/
	public function decreaseFileIDsBeyondMax32bits(){

	}
}
