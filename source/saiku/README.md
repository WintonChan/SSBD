<a href="#readme"></a>

<p align="center">
  <img src="https://raw.githubusercontent.com/OSBI/saiku/assets/L.png" align="left">
  <img src="https://raw.githubusercontent.com/OSBI/saiku/assets/R.png" align="right">
  <b>
    When something doesn't work as expected, then please subscribe to the 
    <a href="https://groups.google.com/a/saiku.meteorite.bi/forum/#!forum/user">Saiku User Group list</a> 
    and send your doubt. If that doesn't solve your problem, then please ask for help on 
    <a href="https://groups.google.com/a/saiku.meteorite.bi/forum/#!forum/dev">Saiku Dev Group</a>.
  </b>
</p>
***

<h1 align="center">Saiku Analytics</h1>
<h2 align="center">The world's greatest open source OLAP browser</h2>
<p align="center"><a href="http://www2.meteorite.bi/saiku-demo/"><img src="https://raw.githubusercontent.com/OSBI/saiku/assets/saiku-demo-1.jpg"/></a></p>
<hr />
<p align="center">
  <a href="http://www.meteorite.bi/"><b>homepage</b></a> |
  <a href="http://community.meteorite.bi/"><b>community</b></a> |
  <a href="http://wiki.meteorite.bi/display/SAIK/Saiku"><b>wiki</b></a> |
  <a href="https://groups.google.com/a/saiku.meteorite.bi/forum/#!forum/user"><b>mailing list</b></a> |
  <a href="http://webchat.freenode.net/?channels=##saiku"><b>chat</b></a> |
  <a href="https://twitter.com/SaikuAnalytics"><b>news</b></a>
</p>
***

<p align="justify">
  Saiku allows business users to explore complex data sources, 
  using a familiar drag and drop interface and easy to understand 
  business terminology, all within a browser. Select the data you 
  are interested in, look at it from different perspectives, 
  drill into the detail. Once you have your answer, save your results, 
  share them, export them to Excel or PDF, all straight from the browser.
  <a href="http://www.meteorite.bi/">(more)</a>
</p>
***

<p align="center">
  <b>
    Please consider supporting development by making a
    <a href="http://www.meteorite.bi/products/saiku/sponsorship">donation</a>.
  </b>
</p>
***

## Setup

### Build Instructions

mvn clean install -DskipTests

mvn clean clover2:setup test clover2:aggregate clover2:clover

## License

Saiku is open source and free to use. Our default server does ship with a license server installed. To get a license you can visit http://licensing.meteorite.bi and get a FREE license which is pinned to the major release of the server, but does not expire at any time. This helps us with a more accurate picture of installation numbers and deployments.

## Wiki

* [Saiku Wiki](http://wiki.meteorite.bi/display/SAIK/Saiku)

## Community

* [Saiku Community](http://community.meteorite.bi/)

## Bugs and Feature Requests

* [Saiku Jira](http://jira.meteorite.bi/)

## Discussion List

* [Saiku Analytics on Slack](http://chat.meteorite.bi/)
* [Saiku Dev Group](https://groups.google.com/a/saiku.meteorite.bi/forum/#!forum/dev)
* [Saiku User Group](https://groups.google.com/a/saiku.meteorite.bi/forum/#!forum/user)
* [<strike>Saiku Forums</strike>](http://forums.meteorite.bi/)
* [Stack Overflow](http://stackoverflow.com/questions/tagged/saiku)
* [Freenode IRC - Channel: #saiku](http://webchat.freenode.net/?channels=##saiku)

## Contributing

Check [CONTRIBUTING.md](https://github.com/OSBI/saiku/blob/master/CONTRIBUTING.md) for more details. Some important information:

* To get started, [sign the Contributor License Agreement](https://www.clahub.com/agreements/OSBI/saiku-ui).

* If you find a bug then please report it on [Jira](http://jira.meteorite.bi/secure/Dashboard.jspa) or our [Support Forum](http://forums.meteorite.bi/).

* If you have a feature request, then please get in touch. We'd love to hear from you! Either post to our [forum](http://forums.meteorite.bi/t/saiku-3-and-beyond/9) or email: [info@meteorite.bi](mailto:info@meteorite.bi)

## History

For detailed changelog, check [Releases](https://github.com/OSBI/saiku/releases).

## License

Saiku and the Saiku UI are free software. The UI, contained in this repository,
is available under the terms of the Apache License Version 2. A copy is attached for your convenience.

## Update project version

To update the pom versions run: 

mvn versions:set -DnewVersion=3.x.x

Then remove the backups with:

 find . -name "*.versionsBackup" -type f -delete
