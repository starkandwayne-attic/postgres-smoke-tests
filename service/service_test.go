package service_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"postgres-smoke-tests/Godeps/_workspace/src/github.com/pborman/uuid"

	"postgres-smoke-tests/Godeps/_workspace/src/github.com/cloudfoundry-incubator/cf-test-helpers/cf"
	"postgres-smoke-tests/Godeps/_workspace/src/github.com/cloudfoundry-incubator/cf-test-helpers/runner"
	"postgres-smoke-tests/Godeps/_workspace/src/github.com/cloudfoundry-incubator/cf-test-helpers/services"

	. "postgres-smoke-tests/Godeps/_workspace/src/github.com/onsi/ginkgo"
	. "postgres-smoke-tests/Godeps/_workspace/src/github.com/onsi/gomega"
	. "postgres-smoke-tests/Godeps/_workspace/src/github.com/onsi/gomega/gbytes"
	. "postgres-smoke-tests/Godeps/_workspace/src/github.com/onsi/gomega/gexec"
)

//Number of insertions to make into the database when insertion testing.
const NUM_INSERTIONS int = 10
const MAX_KEY_LENGTH int32 = 30

type postgresTestConfig struct {
	services.Config

	ServiceName string   `json:"service_name"`
	PlanNames   []string `json:"plan_names"`
}

func loadConfig() (testConfig postgresTestConfig) {
	path := os.Getenv("CONFIG_PATH")
	configFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&testConfig)
	if err != nil {
		panic(err)
	}

	return testConfig
}

var config = loadConfig()

var _ = Describe("RDPG Service Broker", func() {
	var timeout = time.Second * 60
	var retryInterval = time.Second / 2
	var appPath = "../assets/postgres-test-app"

	var appName string

	randomServiceName := func() string {
		return uuid.NewRandom().String()
	}

	//Right now, only testing strings with alphanumeric characters. International character guarantees aren't made yet... and also
	// things will get really messy with JSON if certain other characters can get in the mix (I'm looking at you, horizontal tab).
	validChars := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

	randomName := func() string {
		length := rand.Int31n(MAX_KEY_LENGTH) + 1
		ret := ""
		var i int32
		for i = 0; i < length; i++ {
			ret = ret + validChars[rand.Int31n(62)]
		}
		return ret
	}

	randomNameStartingWithLetter := func() string {
		length := rand.Int31n(MAX_KEY_LENGTH) + 1
		ret := validChars[rand.Int31n(52)] //Total lower/uppercase letters
		var i int32
		for i = 1; i < length; i++ {
			ret = ret + validChars[rand.Int31n(62)]
		}
		return ret
	}

	//Returns the regular expression that matches the expected return value of the curled query requesting a row.
	rowRegexp := func(key string, value string) string {
		return fmt.Sprintf("\\[\\[\"%s\",\"%s\"\\]\\]", key, value)
	}

	appUri := func(appName string) string {
		return "https://" + appName + "." + config.AppsDomain
	}

	assertAppIsRunning := func(appName string) {
		pingUri := appUri(appName) + "/ping"
		fmt.Println("Checking that the app is responding at url: ", pingUri)
		Eventually(runner.Curl(pingUri, "-k"), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
		fmt.Println("\n")
	}

	BeforeSuite(func() {
		config.TimeoutScale = 3
		services.NewContext(config.Config, "rdpg-postgres-smoke-test").Setup()
	})

	BeforeEach(func() {
		appName = randomServiceName()
		Eventually(cf.Cf("push", appName, "-m", "256M", "-p", appPath, "-s", "cflinuxfs2", "--no-start"), config.ScaledTimeout(timeout)).Should(Exit(0))
	})

	AfterEach(func() {
		Eventually(cf.Cf("delete", appName, "-f"), config.ScaledTimeout(timeout)).Should(Exit(0))
	})

	AssertLifeCycleBehavior := func(planName string) {
		It("can create, bind to, write to, read from, unbind, and destroy a service instance using the "+planName+" plan", func() {
			serviceInstanceName := randomServiceName()

			Eventually(cf.Cf("create-service", config.ServiceName, planName, serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			Eventually(cf.Cf("bind-service", appName, serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			Eventually(cf.Cf("start", appName), config.ScaledTimeout(5*time.Minute)).Should(Exit(0))
			assertAppIsRunning(appName)

			//Successful endpoint calls respond 200 and their first line is "SUCCESS"

			//Can't get the timestamp from the database if a connection wasn't made.
			uri := appUri(appName) + "/timestamp"
			fmt.Println("\n--Checking if a connection to the database can be made: ", uri)
			Eventually(runner.Curl(uri, "-k", "-X", "GET"), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			fmt.Println("\n")

			//Can we create a schema in the database?
			uri = appUri(appName) + "/exec"
			schemaName := randomNameStartingWithLetter()
			//Hardly an exhaustive list of verboten schema names, but these two are always known to already exist
			for strings.ToLower(schemaName) == "public" || strings.ToLower(schemaName) == "bdr" {
				schemaName = randomNameStartingWithLetter()
			}

			fmt.Println("\n--Creating new schema: " + schemaName)
			sql := fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			//Can we make a table in the public schema? What about the schema we just made?
			publicTableName := randomNameStartingWithLetter()
			fmt.Println("\n--Creating table in public schema: public." + publicTableName)
			sql = fmt.Sprintf("CREATE TABLE public.%s (key varchar(255) PRIMARY KEY, value int);", publicTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			schemaTableName := randomNameStartingWithLetter()
			fmt.Printf("\n--Creating table in user-created schema: %s.%s\n", schemaName, schemaTableName)
			sql = fmt.Sprintf("CREATE TABLE %s.%s (key varchar(255) PRIMARY KEY, value int);", schemaName, schemaTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			//Let's do some insertions to both tables
			//First, let's generate the values to insert. I've decided on random values, because the database SHOULD be able to handle that...
			//  and I hope don't end up regretting that decision for testing reasons.
			var valuesToInsert [NUM_INSERTIONS]string
			for i := 0; i < NUM_INSERTIONS; i++ {
				valuesToInsert[i] = randomName()
				//Tests would get sort of ugly if any of these, by some unlikely chance, ended up being the same key string
				//Incoming theta(n^3) average-case(n^2) fun.
				for j := 0; j < i; j++ {
					restart := false
					for valuesToInsert[i] == valuesToInsert[j] {
						restart = true
						valuesToInsert[i] = randomName()
					}
					if restart {
						//Hack alert
						//Need to check everything again if we change the value.
						j = -1
					}
				}
			}

			//Now try to throw them into a public-schema table
			fmt.Printf("\n--Inserting %d entries into public.%s\n", NUM_INSERTIONS, publicTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("INSERT INTO public.%s VALUES('%s', %d);", publicTableName, valuesToInsert[i], i)
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			}

			//And try to throw them into a user-created table
			fmt.Printf("\n--Inserting %d entries into user-created table %s.%s\n", NUM_INSERTIONS, schemaName, schemaTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("INSERT INTO %s.%s VALUES('%s', %d);", schemaName, schemaTableName, valuesToInsert[i], i)
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			}

			//Now, poll the values from the database
			fmt.Printf("\n--Polling each inserted entry to verify its presence in public.%s\n", publicTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("SELECT * FROM public.%s WHERE key='%s';", publicTableName, valuesToInsert[i])
				//A JSON representation of the returned row.
				expectedOutput := rowRegexp(valuesToInsert[i], strconv.Itoa(i))
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say(expectedOutput))
			}

			fmt.Printf("\n--Polling each inserted entry to verify its presence in %s.%s\n", schemaName, schemaTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("SELECT * FROM %s.%s WHERE key='%s';", schemaName, schemaTableName, valuesToInsert[i])
				//A JSON representation of the returned row.
				expectedOutput := rowRegexp(valuesToInsert[i], strconv.Itoa(i))
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say(expectedOutput))
			}

			//Time to update some rows
			fmt.Printf("\n--Updating the values of each of the rows in public.%s\n", publicTableName)
			//Originally, all the 'values' should be from 0 to NUM_INSERTIONS (non-inclusive). I guess an update would be to increase all of these by one.
			//This could definitely fail if two keys ended up being the same...
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("UPDATE public.%s SET value=%s WHERE key='%s';", publicTableName, strconv.Itoa(i+1), valuesToInsert[i])
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			}
			//Do the same to the user-created schema table
			fmt.Printf("\n--Updating the values of each of the rows in %s.%s\n", schemaName, schemaTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("UPDATE %s.%s SET value=%s WHERE key='%s';", schemaName, schemaTableName, strconv.Itoa(i+1), valuesToInsert[i])
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			}

			//Now, poll the values from the database
			fmt.Printf("\n--Polling each inserted entry to verify its presence in public.%s\n", publicTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("SELECT * FROM public.%s WHERE key='%s';", publicTableName, valuesToInsert[i])
				//A JSON representation of the returned row.
				expectedOutput := rowRegexp(valuesToInsert[i], strconv.Itoa(i+1))
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say(expectedOutput))
			}

			fmt.Printf("\n--Polling each inserted entry to verify its presence in %s.%s\n", schemaName, schemaTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("SELECT * FROM %s.%s WHERE key='%s';", schemaName, schemaTableName, valuesToInsert[i])
				//A JSON representation of the returned row.
				expectedOutput := rowRegexp(valuesToInsert[i], strconv.Itoa(i+1))
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say(expectedOutput))
			}

			//Make sure that an update is an update and not an insertion. This feels more like a database test than a deployment test, but can't hurt.
			//If it's adding too much time for some reason, this can be commented out later, or something.
			fmt.Printf("\n--Verifying that the pre-updated values are not still in public.%s\n", publicTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("SELECT * FROM public.%s WHERE key='%s';", publicTableName, valuesToInsert[i])
				avoidedOutput := rowRegexp(valuesToInsert[i], strconv.Itoa(i))
				//Not sure if this should be kept as an eventually... once might be enough... but if it is, then this should only act once anyway.
				Consistently(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).ShouldNot(And(Say(avoidedOutput), Say("FAILURE")))
			}

			fmt.Printf("\n--Verifying that the pre-updated values are not still in %s.%s\n", schemaName, schemaTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("SELECT * FROM %s.%s WHERE key='%s';", schemaName, schemaTableName, valuesToInsert[i])
				avoidedOutput := rowRegexp(valuesToInsert[i], strconv.Itoa(i))
				//Again... eventually...?
				Consistently(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).ShouldNot(Or(Say(avoidedOutput), Say("FAILURE")))
			}

			//Clear out all the entries
			fmt.Printf("\n--Clearing all the table entries out of public.%s\n", publicTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("DELETE FROM public.%s WHERE key='%s';", publicTableName, valuesToInsert[i])
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			}

			fmt.Printf("\n--Clearing all the table entries out of %s.%s\n", schemaName, schemaTableName)
			for i := 0; i < NUM_INSERTIONS; i++ {
				sql = fmt.Sprintf("DELETE FROM %s.%s WHERE key='%s';", schemaName, schemaTableName, valuesToInsert[i])
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			}

			//Make sure all of the entries were actually cleared
			fmt.Printf("\n--Verifying that the entries are no longer present in public.%s\n", publicTableName)
			sql = fmt.Sprintf("SELECT * FROM public.%s;", publicTableName)
			//The query shouldn't return any rows, and the app, as it stands, if no rows are returned, won't display any JSON at all - not
			// even an empty JSON array - the code path is short-circuited out. Soooo, there shouldn't be any brackets at all.
			Consistently(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).ShouldNot(Or(Say("\\["), Say("FAILURE")))

			fmt.Printf("\n--Verifying that the entries are no longer present in %s.%s\n", schemaName, schemaTableName)
			sql = fmt.Sprintf("SELECT * FROM %s.%s;", schemaName, schemaTableName)
			Consistently(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).ShouldNot(Or(Say("\\["), Say("FAILURE")))

			//Drop those tables
			fmt.Printf("\n--Dropping table public.%s\n", publicTableName)
			sql = fmt.Sprintf("DROP TABLE public.%s;", publicTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			fmt.Printf("\n--Dropping table %s.%s\n", schemaName, schemaTableName)
			sql = fmt.Sprintf("DROP TABLE %s.%s;", schemaName, schemaTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			//Polling these tables should be a FAILURE because they no longer exist.
			fmt.Printf("\n--Verifying that table public.%s\n was deleted", publicTableName)
			sql = fmt.Sprintf("SELECT * FROM public.%s;", publicTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("FAILURE"))

			fmt.Printf("\n--Verifying that table %s.%s\n was deleted", schemaName, schemaTableName)
			sql = fmt.Sprintf("SELECT * FROM %s.%s;", schemaName, schemaTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("FAILURE"))

			//Drop the schema that was created earlier
			fmt.Printf("\n--Dropping schema %s\n", schemaName)
			sql = fmt.Sprintf("DROP SCHEMA %s;", schemaName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			//Make sure that schema is actually gone
			fmt.Printf("\n--Verifying that schema %s was dropped.", schemaName)
			sql = fmt.Sprintf("SELECT schema_name FROM information_schema.schemata WHERE schema_name='%s';", schemaName)
			Consistently(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).ShouldNot(Or(Say("FAILURE"), Say("\\[")))
			/* THESE ARE REALLY ONLY VALID FOR THE BDR DATABASE... AND THATS GOING AWAY SO... COMMENTED OUT
			//Time to start doing some stuff that the database should not allow
			fmt.Printf("\n--Attempting to create SCHEMA bdr. Failure if this is allowed.")
			sql = fmt.Sprintf("CREATE SCHEMA bdr;")
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("FAILURE"))

			fmt.Printf("\n--Attempting to drop SCHEMA bdr. Failure if this is allowed.")
			sql = fmt.Sprintf("DROP SCHEMA bdr;")
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("FAILURE"))

			fmt.Printf("\n--Attempting to create table table in schema bdr")
			sql = fmt.Sprintf("CREATE TABLE bdr.asdf(key varchar(255), value varchar(255));")
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("FAILURE"))
			*/
			//TODO: Test if table in bdr schema can be modified
			//TODO: Test if table can be dropped from bdr schema
			//These can't be done yet - pending the tables of bdr actually becoming visible.

			Eventually(cf.Cf("unbind-service", appName, serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			Eventually(cf.Cf("delete-service", "-f", serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
		})
	}

	Context("for each plan", func() {
		for _, planName := range config.PlanNames {
			AssertLifeCycleBehavior(planName)
		}
	})
})
