'use strict';

module.exports = function (grunt) {

    grunt.initConfig({
        jshint: {
            src: {
                options: {
                    jshintrc: 'config/jshint/src.json'
                },
                src: [
                    '*.js',
                    'src/**/*.js'
                ]
            },
            test: {
                options: {
                    jshintrc: 'config/jshint/test.json'
                },
                src: [
                    'test/**/*.js'
                ]
            }
        },
        mochaTest: {
            all: {
                options: {
                    reporter: 'spec',
                    require: 'test/globals.js',
                    timeout: 0
                },
                src: [
                    'test/unit/**/*.js'
                ]
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-mocha-test');

    grunt.registerTask('lint', [
        'jshint'
    ]);
    grunt.registerTask('test', [
        'mochaTest'
    ]);

};
