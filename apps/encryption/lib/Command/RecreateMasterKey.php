<?php
/**
* @author Sujith Haridasan <sharidasan@owncloud.com>
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


namespace OCA\Encryption\Command;

use OC\Encryption\Exceptions\DecryptionFailedException;
use OC\Encryption\Manager;
use OC\Files\View;
use OCA\Encryption\KeyManager;
use OCA\Encryption\Util;
use OCP\App\IAppManager;
use OCP\IAppConfig;
use OCP\IConfig;
use OCP\ISession;
use OCP\IUserManager;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class RecreateMasterKey extends Command {

	/** @var Manager  */
	protected $encryptionManager;

	/** @var IUserManager  */
	protected $userManager;

	/** @var View  */
	protected $rootView;

	/** @var KeyManager  */
	protected $keyManager;

	/** @var Util  */
	protected $util;

	/** @var  IAppManager */
	protected $IAppManager;

	/** @var  IAppConfig */
	protected $appConfig;

	/** @var IConfig  */
	protected $IConfig;

	/** @var ISession  */
	protected $ISession;

	/** @var array files which couldn't be decrypted */
	protected $failed;

	/**
	 * RecreateMasterKey constructor.
	 *
	 * @param Manager $encryptionManager
	 * @param IUserManager $userManager
	 * @param View $rootView
	 * @param KeyManager $keyManager
	 * @param Util $util
	 * @param IAppManager $IAppManager
	 * @param IAppConfig $appConfig
	 * @param IConfig $IConfig
	 * @param ISession $ISession
	 */
	public function __construct(Manager $encryptionManager, IUserManager $userManager,
								View $rootView, KeyManager $keyManager, Util $util,
								IAppManager $IAppManager, IAppConfig $appConfig,
								IConfig $IConfig, ISession $ISession) {

		parent::__construct();
		$this->encryptionManager = $encryptionManager;
		$this->userManager = $userManager;
		$this->rootView = $rootView;
		$this->keyManager = $keyManager;
		$this->util = $util;
		$this->IAppManager = $IAppManager;
		$this->appConfig = $appConfig;
		$this->IConfig = $IConfig;
		$this->ISession = $ISession;
	}

	protected function configure() {
		parent::configure();

		$this
			->setName('encryption:recreate-master-key')
			->setDescription('Replace existing master key with new one. Encrypt the file system with newly created master key')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		if ($this->util->isMasterKeyEnabled()) {
			$output->writeln("Decryption started\n");
			$progress = new ProgressBar($output);
			$progress->start();
			$progress->setMessage("Decryption progress...");
			$progress->advance();
			$this->decryptAllUsersFiles($progress);
			$progress->finish();

			if (empty($this->failed)) {

				$this->IAppManager->disableApp('encryption');

				//Delete the files_encryption dir
				$this->rootView->deleteAll('files_encryption');

				$this->appConfig->setValue('core', 'encryption_enabled', 'no');
				$this->appConfig->deleteKey('encryption', 'useMasterKey');
				$this->appConfig->deleteKey('encryption', 'masterKeyId');
				$this->appConfig->deleteKey('encryption', 'recoveryKeyId');
				$this->appConfig->deleteKey('encryption', 'publicShareKeyId');
				$this->appConfig->deleteKey('files_encryption', 'installed_version');

			}
			$output->writeln("\nDecryption completed\n");

			//Reencrypt again
			$this->IAppManager->enableApp('encryption');
			$this->appConfig->setValue('core', 'encryption_enabled', 'yes');
			$this->appConfig->setValue('encryption', 'enabled', 'yes');
			$output->writeln("Encryption started\n");

			$output->writeln("Waiting for creating new masterkey\n");

			$this->keyManager->setPublicShareKeyIDAndMasterKeyId();

			$output->writeln("New masterkey created successfully\n");

			$this->appConfig->setValue('encryption', 'enabled', 'yes');
			$this->appConfig->setValue('encryption', 'useMasterKey', '1');

			$this->keyManager->validateShareKey();
			$this->keyManager->validateMasterKey();
			$progress->setMessage("Encryption progress...");
			$progress->start();
			$progress->advance();
			$this->encryptAllUsersFiles($progress);
			$progress->finish();
			$output->writeln("\nEncryption completed successfully\n");
		} else {
			$output->writeln("Master key is not enabled. Kindly enable it\n");
		}
	}

	public function encryptAllUsersFiles(ProgressBar $progress) {
		$userNo = 1;
		foreach($this->userManager->getBackends() as $backend) {
			$limit = 500;
			$offset = 0;
			do {
				$users = $backend->getUsers('', $limit, $offset);
				foreach ($users as $user) {
					if($this->encryptionManager->isReadyForUser($user)) {
						$this->encryptUsersFiles($user, $progress);
						$progress->advance();
					}
					$userNo++;
				}
				$offset += $limit;
			} while(count($users) >= $limit);
		}
	}

	public function encryptUsersFiles($uid, ProgressBar $progress) {

		$this->setupUserFS($uid);
		$directories = [];
		$directories[] =  '/' . $uid . '/files';

		while($root = array_pop($directories)) {
			$content = $this->rootView->getDirectoryContent($root);
			foreach ($content as $file) {
				$path = $root . '/' . $file['name'];
				if ($this->rootView->is_dir($path)) {
					$directories[] = $path;
					continue;
				} else {
					if($this->encryptFile($path) === false) {
						$progress->setMessage("encrypt files for user $uid: $path (already encrypted)");
						$progress->advance();
					}
				}
			}
		}
	}

	public function encryptFile($path) {
		$source = $path;
		$target = $path . '.encrypted.' . time();

		try {
			$this->ISession->set('encryptAllCmd', true);
			$this->rootView->copy($source, $target);
			$this->rootView->rename($target, $source);
			$this->ISession->remove('encryptAllCmd');
		} catch (DecryptionFailedException $e) {
			if ($this->rootView->file_exists($target)) {
				$this->rootView->unlink($target);
			}
			return false;
		}

		return true;
	}

	protected function decryptAllUsersFiles(ProgressBar $progress) {
		$userList = [];

		foreach ($this->userManager->getBackends() as $backend) {
			$limit = 500;
			$offset = 0;
			do {
				$users = $backend->getUsers('', $limit, $offset);
				foreach ($users as $user) {
					$userList[] = $user;
				}
				$offset += $limit;
			} while (count($users) >= $limit);
		}

		$userNo = 1;
		foreach ($userList as $uid) {
			$this->decryptUsersFiles($uid, $progress);
			$progress->advance();
			$userNo++;
		}
		return true;
	}

	protected function decryptUsersFiles($uid, ProgressBar $progress) {
		$this->setupUserFS($uid);
		$directories = [];
		$directories[] = '/' . $uid . '/files';

		while ($root = array_pop($directories)) {
			$content = $this->rootView->getDirectoryContent($root);
			foreach ($content as $file) {
				// only decrypt files owned by the user
				if($file->getStorage()->instanceOfStorage('OCA\Files_Sharing\SharedStorage')) {
					continue;
				}
				$path = $root . '/' . $file['name'];
				if ($this->rootView->is_dir($path)) {
					$directories[] = $path;
					continue;
				} else {
					try {
						if ($file->isEncrypted() !== false) {
							if ($this->decryptFile($path) === false) {
								$progress->setMessage("decrypt files for user $uid: $path (already decrypted)");
								$progress->advance();
							}
						}
					} catch (\Exception $e) {
						if (isset($this->failed[$uid])) {
							$this->failed[$uid][] = $path;
						} else {
							$this->failed[$uid] = [$path];
						}
					}
				}
			}
		}

		if (empty($this->failed)) {
			$this->rootView->deleteAll("$uid/files_encryption");
		}
	}

	protected function decryptFile($path) {

		$source = $path;
		$target = $path . '.decrypted.' . $this->getTimestamp();

		try {
			$this->ISession->set('decryptAllCmd', true);
			$this->rootView->copy($source, $target);
			$this->rootView->rename($target, $source);
			$this->keyManager->setVersion($source,0, $this->rootView);
			$this->ISession->remove('decryptAllCmd');
		} catch (DecryptionFailedException $e) {
			if ($this->rootView->file_exists($target)) {
				$this->rootView->unlink($target);
			}
			return false;
		}

		return true;
	}

	protected function getTimestamp() {
		return time();
	}

	protected function setupUserFS($uid) {
		\OC_Util::tearDownFS();
		\OC_Util::setupFS($uid);
	}
}
