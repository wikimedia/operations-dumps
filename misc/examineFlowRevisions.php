<?php

require_once __DIR__ . '/Maintenance.php';

class Examiner extends Maintenance {
	public function __construct() {
		parent::__construct();
		$this->addDescription( 'Displays revision info, text info and content for a given revisionid' );
		$this->addOption( 'flowrevid', 'The revision id for which to retrieve info, in b36 encoded format' );
		$this->addOption( 'showtexts', 'Show the text content, not just the metadata' );
		$this->addOption( 'silent', 'Show only errors and the text content' );
	}

	public function execute() {
		if ( $this->hasOption ( 'flowrevid' ) ) {
			$flowRevisionID = mw_hex2bin( mw_alnum2hex( $this->getOption( 'flowrevid' ) ) );
		} else {
			die("Usage: examineFlowRevisions.php [--showtexts] --flowrevid revid (alphanum format)\n");
		}
		if ( $this->hasOption ( 'showtexts' ) ) {
			$showtexts = 1;
		} else {
			$showtexts = 0;
		}
		if ( $this->hasOption( 'silent' ) ) {
			$silent = true;
		} else {
			$silent = false;
		}
		$row=getFlowRevisionInfoFromID($flowRevisionID);
		if ($row) {
			if ( !$silent ) {
				dumpFlowRevisionInfo($row);
			}
			getRevContentFromUrl($row->rev_content,$row->rev_flags, $showtexts, $row->rev_user_wiki, $silent);
		} else {
			die("Failed to retrieve row for revid " . $this->getOption( 'flowrevid' ) . "\n");
		}
	}
}


function getFlowRevisionInfoFromID($flowRevisionID) {
	global $wgFlowCluster;
        global $wgFlowDefaultWikiDb;
	$lb = wfGetLBFactory()->getExternalLB( $wgFlowCluster, $wgFlowDefaultWikiDb );
	$dbr = $lb->getConnection( DB_SLAVE, [], $wgFlowDefaultWikiDb );
	$row = $dbr->selectRow( 'flow_revision', [ 'rev_id', 'rev_flags', 'rev_content', 'rev_parent_id', 'rev_content_length', 'rev_user_wiki' ],
			 array( 'rev_id' => $flowRevisionID )
                      );
	return($row);
}

function dumpFlowRevisionInfo($row) {
	print ("alnum(rev_id): ". mw_hex2alnum(mw_bin2hex($row->rev_id)) . "\n");
	print ("rev_flags: ". $row->rev_flags . "\n");
	print ("rev_content: ". $row->rev_content . "\n");
	print ("rev_content_length: ". $row->rev_content_length . "\n");
	print ("rev_user_wiki: ". $row->rev_user_wiki . "\n");
}

function fetchTextFromExternal($textUrl, $wiki, $silent) {
	// format like
	// DB://clustername/objectid/itemid

	if ( substr($textUrl, 0, 5 ) != "DB://") {
		print "WARNING: bad format text Url, giving up: " . $textUrl . "\n";
		return "";
	}

	$path = explode( '/', $textUrl );
	$cluster  = $path[2];
	$id   = $path[3];

	$lb2 = wfGetLBFactory()->getExternalLB( $cluster );
	$dbr = $lb2->getConnection( DB_SLAVE, [], $wiki );
	$es = ExternalStore::getStoreObject('DB');
	$text = $dbr->selectField( $es->getTable( $dbr ), 'blob_text', array( 'blob_id' => $id ) );
	if ( !$text) {
		print "WARNING: empty or null text retrieved from ext.\n";
		return "";
	} else {
		$textObj = unserialize($text);
		if ($textObj) {
			print "Successfully unserialized text retrieved from ext\n";
			print "returning empty text til we figure out what to do with these objects\n";
			print_r($textObj);
#			return($textObj->...);
			return("");
		}
		else {
			if ( !$silent ) {
				print "WARNING: tried to unserialize text retrieved from ext, seems not to be obj.\n";
			}
			return $text;
		}
	}
}

function processTextFlags($textFlags, $textContent, $silent) {
  if( isset( $textFlags ) ) {
	  if ( in_array( 'gzip', $textFlags ) ) {
		  $text=@gzinflate($textContent);
		  if ($text) {
			if ( !$silent ) {
				print "successfully uncompressed text\n";
			}
			return($text);
		  }
		  else {
			print "WARNING: gzip set but failed to decompress text\n";
			return($textContent);
		  };
	  }
	  else {
		  // shouldn't work but you never know. we have some broken content in there.
		  $text=@gzinflate($textContent);
		  if ($text) {
			print "WARNING: gzip flag not set but text was compressed anyhow\n";
			return($text);
		  }
		  else {
			return($textContent);
		  };
	  }
  }
}

function getRevContentFromUrl( $textUrl, $textFlagsString, $showtexts, $wiki, $silent ) {
	// format like
	// DB://clustername/objectid/itemid

	if ( substr($textUrl, 0, 5 ) == "DB://") {
		$path = explode( '/', $textUrl );
		$cluster  = $path[2];
		$lb2 = wfGetLBFactory()->getExternalLB( $cluster );
		$dbr = $lb2->getConnection( DB_SLAVE, [], $wiki );
        }
        else {
		$dbr = null;
		$id = null;
        }
	$textFlags = explode( ',', $textFlagsString );
	// if it's external then we need to retrieve it first.
	if (in_array( 'external', $textFlags ) ) {
		$text=fetchTextFromExternal($textUrl, $wiki, $silent);
		if ($text) {
			if ( !$silent ) {
				print "external text, url $textUrl, retrieved successfully.\n";
			}
			$textContent = $text;
		}
		else {
			print "WARNING: external text, url $textUrl, failed to retrieve.\n";
			$textContent = "";
		}
	} else {
		$textContent = $textUrl;
	}
	$text = processTextFlags( $textFlags, $textContent, $silent );
	if ( !$silent ) {
		print "here is the text:\n";
	}
	print $text;
	if ( !$silent ) {
		print "\n";
	}
}

function mw_alnum2hex( $alnum ) {
	return str_pad( Wikimedia\base_convert( $alnum, 36, 16 ), 22, '0', STR_PAD_LEFT );
}

function mw_bin2hex( $binary ) {
	return str_pad( bin2hex( $binary ), 22, '0', STR_PAD_LEFT );
}

function mw_hex2alnum( $hex ) {
	return Wikimedia\base_convert( $hex, 16, 36 );
}

function mw_hex2bin( $hex ) {
	return pack( 'H*', $hex );
}


$maintClass = "Examiner";
require_once RUN_MAINTENANCE_IF_MAIN;
