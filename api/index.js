require('dotenv').config();
const path = require('path');
const express = require('express');
const app = express();
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const { v4 } = require('uuid');
const db = require('../connectors/postgres');
const { sendKafkaMessage } = require('../connectors/kafka');
const { validateTicketReservationDto } = require('../validation/reservation');
const messagesType = require('../constants/messages');
const { startKafkaProducer } = require('../connectors/kafka');

// Config setup to parse JSON payloads from HTTP POST request body
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Register the api routes
// HTTP endpoint to test health performance of service
app.get('/api/health', async (req, res) => {
  return res.send('Service Health');
});

// HTTP endpoint to create new user



app.post('/kafka/:type', async (req, res) => {
  const type = req.params.type.toLocaleLowerCase();
  try {
    // validate payload before proceeding with reservations
    const validationError = validateTicketReservationDto(req.body);
    if (validationError) {
      return res.status(403).send(validationError.message);
    }
    if (type === "pending") {
      await sendKafkaMessage(messagesType.TICKET_PENDING, {
        meta: { action: messagesType.TICKET_PENDING },
        body: {
          matchNumber: req.body.matchNumber,
          tickets: req.body.tickets,
        }
      });

    } else if (type === "reserve") {
      await sendKafkaMessage(messagesType.TICKET_RESERVED, {
        meta: { action: messagesType.TICKET_RESERVED },
        body: {
          matchNumber: req.body.matchNumber,
          tickets: req.body.tickets,
        }
      });

    } else if (type === "cancel") {
      await sendKafkaMessage(messagesType.TICKET_CANCELLED, {
        meta: { action: messagesType.TICKET_CANCELLED },
        body: {
          matchNumber: req.body.matchNumber,
          tickets: req.body.tickets,
        }
      });
    } else {
      return res.status(404).json({ error: "Invalid action. Please use one of the following: pending | reserve | cancel." });
    }

    return res.json({
      message: 'Ticket Purchase Successful',
    });
  } catch (e) {
    return res.status(400).send(e.message);
  }
});

// If request doesn't match any of the above routes then return 404
app.use((req, res, next) => {
  return res.status(404).send();
});

// Create HTTP Server and Listen for Requests
app.listen(3009, async (req, res) => {
  console.log("http://localhost:3009");
  await startKafkaProducer();
});


