<?php

class MessageTest extends PHPUnit_Framework_TestCase {

	function setUp() {
		global $wgLanguageCode;
		
		$wgLanguageCode = 'en'; # For mainpage to be 'Main Page'
	}

	function testExists() {
		$this->assertTrue( wfMessage( 'mainpage' )->exists() );
		$this->assertTrue( wfMessage( 'mainpage' )->params( array() )->exists() );
		$this->assertTrue( wfMessage( 'mainpage' )->rawParams( 'foo', 123 )->exists() );
		$this->assertFalse( wfMessage( 'i-dont-exist-evar' )->exists() );
		$this->assertFalse( wfMessage( 'i-dont-exist-evar' )->params( array() )->exists() );
		$this->assertFalse( wfMessage( 'i-dont-exist-evar' )->rawParams( 'foo', 123 )->exists() );
	}

	function testKey() {
		$this->assertType( 'Message', wfMessage( 'mainpage' ) );
		$this->assertType( 'Message', wfMessage( 'i-dont-exist-evar' ) );
		$this->assertEquals( 'Main Page', wfMessage( 'mainpage' )->text() );
		$this->assertEquals( '&lt;i-dont-exist-evar&gt;', wfMessage( 'i-dont-exist-evar' )->text() );
	}

	function testInLanguage() {
		$this->assertEquals( 'Main Page', wfMessage( 'mainpage' )->inLanguage( 'en' )->text() );
		$this->assertEquals( 'Заглавная страница', wfMessage( 'mainpage' )->inLanguage( 'ru' )->text() );
		$this->assertEquals( 'Main Page', wfMessage( 'mainpage' )->inLanguage( Language::factory( 'en' ) )->text() );
		$this->assertEquals( 'Заглавная страница', wfMessage( 'mainpage' )->inLanguage( Language::factory( 'ru' ) )->text() );
	}

	/**
	 * @expectedException MWException
	 */
	function testInLanguageThrows() {
		wfMessage( 'foo' )->inLanguage( 123 );
	}
}
