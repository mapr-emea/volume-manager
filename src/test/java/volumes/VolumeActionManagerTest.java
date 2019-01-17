package volumes;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Unit test for VolumeActionManager
 */
public class VolumeActionManagerTest
    extends TestCase
{
    static private VolumeManagerConfiguration basicTestConf;
    static private VolumeManagerConfiguration retentionTestConf;
    static private VolumeActionManager vam;

    /**
     * Test logger
     */
    private static final Log LOG = LogFactory.getLog(VolumeActionManagerTest.class);

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public VolumeActionManagerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        basicTestConf = new VolumeManagerConfiguration(new File("./src/test/resources/conf.basic"));
        retentionTestConf = new VolumeManagerConfiguration(new File("./src/test/resources/conf.retention"));
        vam = new VolumeActionManager();
        TestSuite suite = new TestSuite();
        suite.addTest(new VolumeActionManagerTest("testBasicDaily"));
        suite.addTest(new VolumeActionManagerTest("testBasicMonthly"));
        suite.addTest(new VolumeActionManagerTest("testBasicYearly"));
        suite.addTest(new VolumeActionManagerTest("testRetentionForever"));
        suite.addTest(new VolumeActionManagerTest("testDefAetype"));
        suite.addTest(new VolumeActionManagerTest("testDefSchedule"));
        return suite;
    }

    // basic test on daily volumes
    public void testBasicDaily() {

        ArrayList<MaprVolume> list = new ArrayList<MaprVolume>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date(System.currentTimeMillis());
        String today = sdf.format(date.getTime());
        String one_week_ago = sdf.format(date.getTime() - 86400*7*1000);
        list.add(new MaprVolume("auto_test_daily_" + today));
        list.add(new MaprVolume("auto_test_daily_" + one_week_ago));
        vam.prepare(basicTestConf, list);
        final List<MaprVolume> createList = vam.getCreateList();
        final List<MaprVolume> purgeList = vam.getPurgeList();
        assertTrue(createList.size() == 14);
        assertTrue(purgeList.size() == 1);
    }

    // basic test on monthly volumes
    public void testBasicMonthly() {

        ArrayList<MaprVolume> list = new ArrayList<MaprVolume>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM01");
        Date date = new Date(System.currentTimeMillis());
        String today = sdf.format(date.getTime());
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, -5);
        String five_month_ago = sdf.format(cal.getTime());
        cal.add(Calendar.MONTH, 10);
        String five_month_ahead = sdf.format(cal.getTime());
        list.add(new MaprVolume("auto_test_monthly_" + today));
        list.add(new MaprVolume("auto_test_monthly_" + five_month_ago));
        list.add(new MaprVolume("auto_test_monthly_" + five_month_ahead));
        vam.prepare(basicTestConf, list);
        final List<MaprVolume> createList = vam.getCreateList();
        final List<MaprVolume> purgeList = vam.getPurgeList();
        assertTrue(createList.size() == 14);
        assertTrue(purgeList.size() == 1);
    }

    // basic test on yearly volumes
    public void testBasicYearly() {

        ArrayList<MaprVolume> list = new ArrayList<MaprVolume>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy0101");
        Date date = new Date(System.currentTimeMillis());
        String today = sdf.format(date.getTime());
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, -5);
        String five_years_ago = sdf.format(cal.getTime());
        cal.add(Calendar.YEAR, 10);
        String five_years_ahead = sdf.format(cal.getTime());
        list.add(new MaprVolume("auto_test_yearly_" + today));
        list.add(new MaprVolume("auto_test_yearly_" + five_years_ago));
        list.add(new MaprVolume("auto_test_yearly_" + five_years_ahead));
        vam.prepare(basicTestConf, list);
        final List<MaprVolume> createList = vam.getCreateList();
        final List<MaprVolume> purgeList = vam.getPurgeList();
        assertTrue(createList.size() == 14);
        assertTrue(purgeList.size() == 1);
    }

    // retention=0 (forever) test
    public void testRetentionForever() {

        ArrayList<MaprVolume> list = new ArrayList<MaprVolume>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date(System.currentTimeMillis());
        String today = sdf.format(date.getTime());
        String one_week_ago = sdf.format(date.getTime() - 86400*7*1000);
        list.add(new MaprVolume("auto_test_daily_" + today));
        list.add(new MaprVolume("auto_test_daily_" + one_week_ago));
        vam.prepare(retentionTestConf, list);
        final List<MaprVolume> createList = vam.getCreateList();
        final List<MaprVolume> purgeList = vam.getPurgeList();
        assertTrue(createList.size() == 12);
        assertTrue(purgeList.size() == 0);
    }

    // test default aetype setting
    public void testDefAetype() {

        LOG.info("testDefAetype");

        VolumeManagerConfiguration defAetypeConf = new VolumeManagerConfiguration(new File("./src/test/resources/conf.defaetype"));
        ArrayList<MaprVolume> list = new ArrayList<MaprVolume>();

        vam.prepare(defAetypeConf, list);
        final List<MaprVolume> createList = vam.getCreateList();
        assertTrue(createList.size() == 5);
        MaprVolume vol = createList.get(0);
        VolumeManager.setConf(defAetypeConf);
        String vcUrl = vam.buildVolumeCreateURL(vol);
        assertTrue(vcUrl.contains("schedule=3"));
    }

    // test default schedule setting
    public void testDefSchedule() {

        LOG.info("testDefSchedule");

        VolumeManagerConfiguration defScheduleConf = new VolumeManagerConfiguration(new File("./src/test/resources/conf.defschedule"));
        ArrayList<MaprVolume> list = new ArrayList<MaprVolume>();

        vam.prepare(defScheduleConf, list);
        final List<MaprVolume> createList = vam.getCreateList();
        assertTrue(createList.size() == 5);
        MaprVolume vol = createList.get(0);
        VolumeManager.setConf(defScheduleConf);
        String vcUrl = vam.buildVolumeCreateURL(vol);
        assertTrue(!vcUrl.contains("schedule=0"));
    }
}
