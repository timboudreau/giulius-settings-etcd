giulius-settings-etcd
---------------------

An extension to [Giulius](https://github.com/timboudreau/giulius) for loading settings from
[etcd](https://github.com/coreos/etcd) using a netty-based fork of [jetcd](https://github.com/timboudreau/jetcd).

Available from [this maven repository](http://timboudreau.com/builds/).

Basically this allows you to have settings (typically used as Guice @Named values) which are periodically
refreshed from one or more instances of Etcd, a high-availability, clustered, atomic key-value store which is
like Apache Zookeeper minus the awfulness.

It also includes:

 * ``MetaEtcdClient`` - a wrapper for several EtcdClient instances pointing at different machines, which can
fail over between them using a simple algorithm in which a connection that fails more than N times in 
M seconds is not queried again until a time-window has elapsed since the last failure, unless all other
clients have also failed (in which case it will retry to the limit, and then throw an exception)
 * ``EtcdModule`` - binds Etcd using Guice

Chicken and Egg
---------------

There is a small chicken-and-egg issue here, in that you are probably using Guice if you're using Settings,
and you need the Settings to create your modules, but you also need a Settings and most likely an injector
to create your ``EtcdSettings``.

The best suggestion is just to punt:  Build your settings without ``EtcdSettings``, create your EtcdSettings
(using Guice under the hood, but it won't interfere with the injector for your application), then build
your settings a second time for production purposes.

		// Load defaults from /etc/my-app.properties, ~/my-app.properties, ./my-app.properties
		// and the classpath
		SettingsBuilder sb = SettingsBuilder.createWithDefaults("my-app");

		// Use those to determine the etcd cluster settings - i.e. urls
		// for servers, etc.
		EtcdSettings clusterSettings = EtcdModule.create(sb.build());

		// Now build the settings the application's injector will use, which
		// use values from etcd, and fallback values from disk/classpth
		Settings s = sb.add(clusterSettings).build();

		// Create our injector
		Dependencies deps = new Dependencies(s, new ModuleOne(), new ModuleTwo(), ...);

		// start the application
		Server server = deps.getInstance(Server.class);
		server.start();

License
-------

MIT license

