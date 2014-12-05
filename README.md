# Native JGroups S3_PING

Native means, it uses the AWS SDK and does not implement the HTTP protocol on its own. The benefit is a more stable
connection as well as usage of IAM server profiles and AWS standardized credential distribution.

# Configuration

Like the original `S3_PING`, this library implement a JGroups `Discovery` protocol which replaces protocols like
`UNICAST` or `TCPPING`.

    <de.zalando.jgroups.NATIVE_S3_PING
            regionName="eu-west-1"
            bucketName="jgroups-s3-test"
            bucketPrefix="jgroups"/>

To be able to use this configuration in your application, you have to initialize the plugin in your code:

    NATIVE_S3_PING.registerProtocolWithJGroups();

## Possible Configurations

* **regionName**: like "eu-west-1", "us-east-1", etc.
* **bucketName**: the S3 bucket to store the files in
* **bucketPrefix** (optional): if you don't want the plugin to pollute your S3 bucket, you can configure a prefix like
  "jgroups/"
* **endpoint** (optional): you can override the S3 endpoint if yuo know what you are doing

## Example Configuration

    <!--
    Based on udp.xml but with NATIVE_S3_PING.
    -->
    <config xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns="urn:org:jgroups"
            xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/jgroups.xsd">
        <UDP
                mcast_port="${jgroups.udp.mcast_port:45588}"
                ip_ttl="0"
                tos="8"
                ucast_recv_buf_size="5M"
                ucast_send_buf_size="5M"
                mcast_recv_buf_size="5M"
                mcast_send_buf_size="5M"
                max_bundle_size="64K"
                max_bundle_timeout="30"
                enable_diagnostics="true"
                thread_naming_pattern="cl"
                timer_type="new3"
                timer.min_threads="2"
                timer.max_threads="4"
                timer.keep_alive_time="3000"
                timer.queue_max_size="500"
                thread_pool.enabled="true"
                thread_pool.min_threads="2"
                thread_pool.max_threads="8"
                thread_pool.keep_alive_time="5000"
                thread_pool.queue_enabled="true"
                thread_pool.queue_max_size="10000"
                thread_pool.rejection_policy="discard"
                oob_thread_pool.enabled="true"
                oob_thread_pool.min_threads="1"
                oob_thread_pool.max_threads="8"
                oob_thread_pool.keep_alive_time="5000"
                oob_thread_pool.queue_enabled="false"
                oob_thread_pool.queue_max_size="100"
                oob_thread_pool.rejection_policy="discard"/>



        <MERGE3 max_interval="30000"
                min_interval="10000"/>
        <FD_SOCK/>
        <FD_ALL/>
        <VERIFY_SUSPECT timeout="1500" />
        <BARRIER />
        <pbcast.NAKACK2 xmit_interval="500"
                        xmit_table_num_rows="100"
                        xmit_table_msgs_per_row="2000"
                        xmit_table_max_compaction_time="30000"
                        max_msg_batch_size="500"
                        use_mcast_xmit="false"
                        discard_delivered_msgs="true"/>

        <de.zalando.jgroups.NATIVE_S3_PING
                regionName="eu-west-1"
                bucketName="jgroups-s3-test"
                bucketPrefix="jgroups"/>

        <pbcast.STABLE stability_delay="1000" desired_avg_gossip="50000"
                       max_bytes="4M"/>
        <pbcast.GMS print_local_addr="true" join_timeout="2000"
                    view_bundling="true"/>
        <UFC max_credits="2M"
             min_threshold="0.4"/>
        <MFC max_credits="2M"
             min_threshold="0.4"/>
        <FRAG2 frag_size="60K" />
        <RSVP resend_interval="2000" timeout="10000"/>
        <pbcast.STATE_TRANSFER />
        <!-- pbcast.FLUSH /-->
    </config>
