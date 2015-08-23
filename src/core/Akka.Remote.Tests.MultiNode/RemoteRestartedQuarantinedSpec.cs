using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Remote.TestKit;
using Akka.Remote.Transport;
using Akka.TestKit;
using Akka.Util.Internal;

namespace Akka.Remote.Tests.MultiNode
{
    public class RemoteRestartedQuarantinedNodeConfig : MultiNodeConfig
    {
        public RoleName First { get; private set; }
        public RoleName Second { get; private set; }

        public RemoteRestartedQuarantinedNodeConfig()
        {
            TestTransport = true;

            First = Role("first");
            Second = Role("second");

            CommonConfig = DebugConfig(true).WithFallback(ConfigurationFactory.ParseString(
            @"
      akka.loglevel = WARNING
      akka.remote.log-remote-lifecycle-events = WARNING

      # Keep it long, we don't want reconnects
      akka.remote.retry-gate-closed-for  = 1 s

      # Important, otherwise it is very racy to get a non-writing endpoint: the only way to do it if the two nodes
      # associate to each other at the same time. Setting this will ensure that the right scenario happens.
      akka.remote.use-passive-connections = off

      # TODO should not be needed, but see TODO at the end of the test
      akka.remote.transport-failure-detector.heartbeat-interval = 1 s
      akka.remote.transport-failure-detector.acceptable-heartbeat-pause = 10 s
           "));
        }
    }

    public class RemoteRestartedQuarantinedMultiNode1 : RemoteRestartedQuarantinedSpec
    {
    }

    public class RemoteRestartedQuarantinedMultiNode2 : RemoteRestartedQuarantinedSpec
    {
    }

    public abstract class RemoteRestartedQuarantinedSpec : MultiNodeSpec
    {
        private readonly RemoteRestartedQuarantinedNodeConfig _config;

        protected RemoteRestartedQuarantinedSpec() : this(new RemoteRestartedQuarantinedNodeConfig())
        {
        }

        protected RemoteRestartedQuarantinedSpec(RemoteRestartedQuarantinedNodeConfig config) : base(config)
        {
            _config = config;
        }

        protected override int InitialParticipantsValueFactory
        {
            get { return 2; } // todo: roles.count ?
        }

        public class Subject : UntypedActor
        {
            protected override void OnReceive(object message)
            {
                if (message.Equals("shutdown")) Context.System.Shutdown();
                else if (message.Equals("identify"))// todo identify instance ?
                {
                    var sendBack = new SubjectIdentifyResponse(AddressUidExtension.Uid(Context.System), Self);
                    Sender.Tell(sendBack);
                }
            }
        }

        public class SubjectIdentifyResponse
        {
            private readonly int _uid;
            private readonly IActorRef _actorRef;

            public SubjectIdentifyResponse(int uid, IActorRef actorRef)
            {
                _uid = uid;
                _actorRef = actorRef;
            }

            public int Uid
            {
                get { return _uid; }
            }

            public IActorRef ActorRef
            {
                get
                {
                    return _actorRef;
                }
            }
        }

        private SubjectIdentifyResponse IdentifyWithUid(RoleName role, string actorName)
        {
            Sys.ActorSelection(Node(role) / "user" / actorName).Tell("identify");
            return ExpectMsg<SubjectIdentifyResponse>();
        }
        
        [MultiNodeFact]
        public void RemoteRestartedQuarantinedSpecs()
        {
            ARestartedQuarantinedSystemShouldNotCrashTheOtherSystem();
        }

        public void ARestartedQuarantinedSystemShouldNotCrashTheOtherSystem()
        {
            Sys.ActorOf<Subject>("subject");
            EnterBarrier("subject-started");

            RunOn(() =>
            {
                var secondAddress = Node(_config.Second).Address;
                
                var uuidRef = IdentifyWithUid(_config.Second, "subject");

                RARP.For(Sys).Provider.Transport.Quarantine(secondAddress, uuidRef.Uid);

                EnterBarrier("quarantined");
                EnterBarrier("still-quarantined");

                TestConductor.Shutdown(_config.Second).Wait();

                Within(TimeSpan.FromSeconds(30), () =>
                {
                    AwaitAssert(() =>
                    {
                        Sys.ActorSelection(new RootActorPath(secondAddress, "subject")).Tell(new Identify("subject"));//TODO disck
                        var testme = ExpectMsg<ActorIdentity>(TimeSpan.FromSeconds(1)).Subject;
                    });
                });

                Sys.ActorSelection(new RootActorPath(secondAddress, "subject")).Tell("shutdown");//TODO disck
            }, _config.First);

            RunOn(() =>
            {
                
                var addr = Sys.AsInstanceOf<ExtendedActorSystem>().Provider.DefaultAddress;
                var firstAddress = Node(_config.First).Address;

                var actorRef = IdentifyWithUid(_config.First, "subject").ActorRef;

                EnterBarrier("quarantined");

                // Check that quarantine is intact
                /*Within(TimeSpan.FromSeconds(10), () =>
                {
                    AwaitAssert(() =>
                    {
                        // todo: no idea
                        EventFilter.Warning(pattern: new Regex("The remote system has quarantined this system"))
                            .ExpectOne(
                                () =>
                                {
                                    actorRef.Tell("boo!");
                                });
                    });
                });*/

                EnterBarrier("still-quarantined");

                Sys.AwaitTermination(TimeSpan.FromSeconds(10));
/*
                var freshSystem = ActorSystem.Create(Sys.Name, ConfigurationFactory.ParseString(String.Format(@"
                    akka.remote.retry-gate-closed-for = 0.5 s
                    akka.remote.netty.tcp {
                      hostname = {0}
                      port = {1}
                    }
                ", addr.Host, addr.Port)));

                var probe = CreateTestProbe(freshSystem);
                freshSystem.ActorSelection(new RootActorPath(firstAddress, "subject"))
                    .Tell(new Identify("subject"), probe.Ref); // todo wat
               // TODO sometimes it takes long time until the new connection is established,
               //      It seems like there must first be a transport failure detector timeout, that triggers
               //      "No response from remote. Handshake timed out or transport failure detector triggered".
                probe.ExpectMsg<ActorIdentity>(TimeSpan.FromSeconds(30)).ShouldNotBe(null);

                // Now other system will be able to pass, too
                freshSystem.ActorOf<Subject>("subject");
                
                freshSystem.TerminationTask.Wait(TimeSpan.FromSeconds(10));
                */
            }, _config.Second);
        }
    }
}
